use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use tokio::time::Instant;
use tokio::time::Duration;
use tokio::sync::oneshot;
use log::info;

use arrow_flight::flight_service_server::FlightService;
use arrow_flight::*;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};

use futures::{stream, Stream, StreamExt};
use prost::Message;
use sail_catalog::temp_view::TemporaryViewManager;
use sail_common::config::{AppConfig, ExecutionMode};
use sail_common::runtime::RuntimeManager;
use sail_execution::driver::DriverOptions;
use sail_execution::job::{ClusterJobRunner, JobRunner, LocalJobRunner};

use sail_spark_connect::spark::connect::{SqlCommand, Relation};
use sail_spark_connect;
use sail_spark_connect::spark::connect::relation;
use sail_spark_connect::spark::connect as sc;
use sail_common::spec;
use sail_plan::extension::analyzer::default_analyzer_rules;
use sail_plan::extension::optimizer::default_optimizer_rules;
use sail_plan::new_query_planner;
use sail_plan::resolve_and_execute_plan;
use sail_server::actor::{Actor, ActorAction, ActorContext, ActorHandle, ActorSystem};
use sail_object_store::DynamicObjectStoreRegistry;

// Import Sail's SQL analyzer for direct SQL parsing
use sail_sql_analyzer;

use tonic::{Request, Response, Status};
use tonic::Streaming;

use crate::error::{SparkError, SparkResult};

type TonicResult<T> = std::result::Result<T, Status>;
use crate::session::{SparkExtension, DEFAULT_SPARK_CATALOG, DEFAULT_SPARK_SCHEMA};

// Import Flight SQL protobuf types
use crate::generated::arrow_flight_protocol_sql::{CommandGetSqlInfo, CommandStatementQuery, SqlInfo};

// Enum to represent different Flight SQL commands
#[derive(Debug)]
enum FlightSqlCommand {
    GetSqlInfo(CommandGetSqlInfo),
    StatementQuery(CommandStatementQuery),
}

type BoxFlightStream<T> = Pin<Box<dyn Stream<Item = std::result::Result<T, Status>> + Send + 'static>>;

#[derive(Clone)]
pub struct SailFlightService {
    queries: Arc<Mutex<HashMap<String, Vec<FlightData>>>>,
    system: Arc<Mutex<ActorSystem>>,
}

impl SailFlightService {
    pub fn new() -> Self {
        let mut queries = HashMap::new();

        let demo_data = vec![
            FlightData {
                flight_descriptor: None,
                data_body: "Hello from Sail Arrow Flight SQL!".as_bytes().to_vec().into(),
                data_header: vec![].into(),
                app_metadata: vec![].into(),
            },
            FlightData {
                flight_descriptor: None,
                data_body: "Demo data row 2".as_bytes().to_vec().into(),
                data_header: vec![].into(),
                app_metadata: vec![].into(),
            },
        ];
        queries.insert("demo_query".to_string(), demo_data);
        
        SailFlightService { 
            queries: Arc::new(Mutex::new(queries)),
            system: Arc::new(Mutex::new(ActorSystem::new())),
        }
    }
    
    async fn execute_sql_query(&self, query: String) -> std::result::Result<Vec<FlightData>, Status> {
        println!("* Executing SQL query: {}", query.to_string());
        
        // Create a proper SqlCommand using the protobuf-generated struct
        let sql_command = SqlCommand {
            sql: query.clone(), // This is the deprecated field, but we'll use it for now
            args: std::collections::HashMap::new(),
            pos_args: vec![],
            named_arguments: std::collections::HashMap::new(),
            pos_arguments: vec![],
            input: None,
        };
        
        // Create a SessionContext with Sail extensions
        let ctx = self.create_sail_session_context().await?;
        self.add_demo_data(&ctx).await?;
        
        // Use Sail's SQL execution flow instead of DataFusion's direct SQL
        match self.execute_sail_sql(&ctx, sql_command).await {
            Ok(stream) => {
                let flight_data_stream = self.convert_to_flight_data(stream)?;
                let mut flight_data = Vec::new();
                tokio::pin!(flight_data_stream);
                while let Some(data) = flight_data_stream.next().await {
                    match data {
                        Ok(data) => flight_data.push(data),
                        Err(e) => return Err(Status::internal(format!("Stream error: {}", e))),
                    }
                }
                
                Ok(flight_data)
            }
            Err(e) => {
                println!("* Sail SQL execution error: {}", e);
                // Return demo data as fallback
                if let Some(data) = self.queries.lock().unwrap().get("demo_query") {
                    Ok(data.clone())
                } else {
                    Ok(vec![])
                }
            }
        }
    }
    
    async fn create_sail_session_context(&self) -> std::result::Result<SessionContext, Status> {
        let config = AppConfig::load().map_err(|e| Status::internal(format!("Failed to load AppConfig: {}", e)))?;
        
        // Create RuntimeManager first, then get the handle
        let runtime_manager = RuntimeManager::try_new(&config.runtime)
            .map_err(|e| Status::internal(format!("Failed to create runtime manager: {}", e)))?;
        let runtime_handle = runtime_manager.handle();
        
        let runtime = {
            let registry = DynamicObjectStoreRegistry::new(runtime_handle.clone());
            let builder = RuntimeEnvBuilder::default().with_object_store_registry(Arc::new(registry));
            Arc::new(builder.build().map_err(|e| Status::internal(format!("Failed to build runtime: {}", e)))?)
        };
        
        // Create a proper Sail session configuration
        let mut session_config = SessionConfig::new()
            .with_create_default_catalog_and_schema(true)
            .with_default_catalog_and_schema(DEFAULT_SPARK_CATALOG, DEFAULT_SPARK_SCHEMA)
            .with_information_schema(false)
            .with_extension(Arc::new(TemporaryViewManager::default()));
        
        // Create job runner based on execution mode
        let job_runner: Box<dyn JobRunner> = match config.mode {
            ExecutionMode::Local => Box::new(LocalJobRunner::new()),
            ExecutionMode::LocalCluster | ExecutionMode::KubernetesCluster => {
                let options = DriverOptions::try_new(&config, runtime_handle.clone())
                    .map_err(|e| Status::internal(format!("Failed to create driver options: {}", e)))?;
                let mut system = self.system.lock().unwrap();
                Box::new(ClusterJobRunner::new(system.deref_mut(), options))
            }
        };
        
        // Add Spark extension with proper session management
        let spark_extension = SparkExtension::try_new(
            None, // user_id - can be None for Flight SQL
            "flight_sql_session".to_string(), // session_id
            job_runner,
        ).map_err(|e| Status::internal(format!("Failed to create Spark extension: {}", e)))?;
        
        session_config = session_config.with_extension(Arc::new(spark_extension));
        
        // Set execution options
        {
            let execution = &mut session_config.options_mut().execution;
            execution.batch_size = config.execution.batch_size;
            execution.collect_statistics = config.execution.collect_statistics;
            execution.listing_table_ignore_subdirectory = false;
        }
        
        let state = SessionStateBuilder::new()
            .with_config(session_config)
            .with_runtime_env(runtime)
            .with_default_features()
            .with_analyzer_rules(default_analyzer_rules())
            .with_optimizer_rules(default_optimizer_rules())
            .with_query_planner(new_query_planner())
            .build();
        
        let context = SessionContext::new_with_state(state);
        Ok(context)
    }
    
    async fn execute_sail_sql(
        &self, 
        ctx: &SessionContext, 
        sql_command: SqlCommand
    ) -> std::result::Result<datafusion::physical_plan::SendableRecordBatchStream, Status> {
        println!("* Executing SQL using Sail's own SQL parser and execution system");
        
        // Use Sail's SQL parser to parse the SQL string directly
        let sql_query = sql_command.sql.clone();
        println!("* Parsing SQL: {}", sql_query);
        
        // Parse the SQL using Sail's parser
        let ast_statement = sail_sql_analyzer::parser::parse_one_statement(&sql_query)
            .map_err(|e| Status::internal(format!("Failed to parse SQL: {}", e)))?;
        
        println!("* Successfully parsed SQL statement");
        
        // Convert AST statement to Sail Plan using Sail's analyzer
        let sail_plan = sail_sql_analyzer::statement::from_ast_statement(ast_statement)
            .map_err(|e| Status::internal(format!("Failed to convert AST to Sail plan: {}", e)))?;
        
        println!("* Successfully converted to Sail plan: {:?}", sail_plan);
        
        // Get the Spark extension to access Sail's configuration
        let spark = SparkExtension::get(ctx)
            .map_err(|e| Status::internal(format!("Failed to get Spark extension: {}", e)))?;
        
        // Use Sail's plan resolution and execution
        let physical_plan = resolve_and_execute_plan(ctx, spark.plan_config()?, sail_plan).await
            .map_err(|e| Status::internal(format!("Failed to resolve and execute Sail plan: {}", e)))?;
        
        println!("* Successfully resolved and executed Sail plan");
        
        // Execute the physical plan using Sail's job runner
        spark.job_runner().execute(ctx, physical_plan).await
            .map_err(|e| Status::internal(format!("Failed to execute physical plan: {}", e)))
    }
    
    async fn add_demo_data(&self, ctx: &SessionContext) -> std::result::Result<(), Status> {
        // Create demo table with some data
        let schema = arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int32, false),
            arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("value", arrow::datatypes::DataType::Float64, false),
        ]);
        
        let id_array = arrow::array::Int32Array::from(vec![1, 2, 3, 4, 5]);
        let name_array = arrow::array::StringArray::from(vec!["Alice", "Bob", "Charlie", "Diana", "Eve"]);
        let value_array = arrow::array::Float64Array::from(vec![10.5, 20.3, 15.7, 25.1, 18.9]);
        
        let record_batch = arrow::record_batch::RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(id_array), Arc::new(name_array), Arc::new(value_array)]
        ).map_err(|e| Status::internal(format!("Failed to create record batch: {}", e)))?;
        
        // Register the table
        ctx.register_batch("demo_table", record_batch)
            .map_err(|e| Status::internal(format!("Failed to register table: {}", e)))?;
        
        println!("* Added demo table with 5 rows");
        Ok(())
    }
    
    fn convert_to_flight_data(
        &self,
        stream: datafusion::physical_plan::SendableRecordBatchStream,
    ) -> std::result::Result<BoxFlightStream<FlightData>, Status> {
        let self_ref = self.clone();
        let flight_stream = stream::unfold(stream, move |mut stream| {
            let self_ref = self_ref.clone();
            async move {
                match stream.next().await {
                    Some(Ok(batch)) => {
                        match self_ref.record_batch_to_flight_data(batch) {
                            Ok(flight_data) => Some((Ok(flight_data), stream)),
                            Err(e) => Some((Err(e), stream)),
                        }
                    }
                    Some(Err(e)) => Some((Err(Status::internal(format!("Record batch error: {}", e))), stream)),
                    None => None,
                }
            }
        });
        Ok(Box::pin(flight_stream))
    }
    
    fn record_batch_to_flight_data(&self, record_batch: arrow::record_batch::RecordBatch) -> std::result::Result<FlightData, Status> {
        // Convert RecordBatch to Arrow IPC format
        let mut buffer = Vec::new();
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut buffer, &record_batch.schema())
            .map_err(|e| Status::internal(format!("Failed to create IPC writer: {}", e)))?;
        
        writer.write(&record_batch)
            .map_err(|e| Status::internal(format!("Failed to write record batch: {}", e)))?;
        
        writer.finish()
            .map_err(|e| Status::internal(format!("Failed to finish IPC writer: {}", e)))?;
        
        // Create FlightData
        let flight_data = FlightData {
            flight_descriptor: None,
            data_body: buffer.into(),
            data_header: vec![].into(), // Arrow IPC format doesn't need separate header
            app_metadata: vec![].into(),
        };
        
        Ok(flight_data)
    }

    fn handle_flight_sql_command(&self, cmd_bytes: &[u8]) -> std::result::Result<FlightSqlCommand, Status> {
        // Check if this is a protobuf Any message with Flight SQL command
        let cmd_str = String::from_utf8_lossy(cmd_bytes);
        
        if cmd_str.contains("type.googleapis.com/arrow.flight.protocol.sql.CommandGetSqlInfo") {
            // Try to decode as CommandGetSqlInfo
            if let Ok(cmd) = CommandGetSqlInfo::decode(cmd_bytes) {
                println!("* Received CommandGetSqlInfo request with {} info types", cmd.info.len());
                return Ok(FlightSqlCommand::GetSqlInfo(cmd));
            }
            
            // Sometimes the protobuf Any wrapper needs to be handled differently
            // For now, create a default CommandGetSqlInfo to return basic server info
            println!("* Creating default CommandGetSqlInfo response");
            return Ok(FlightSqlCommand::GetSqlInfo(CommandGetSqlInfo { info: vec![] }));
        }
        
        // Try to decode as CommandStatementQuery
        if let Ok(cmd) = CommandStatementQuery::decode(cmd_bytes) {
            println!("* Received CommandStatementQuery: {}", cmd.query);
            return Ok(FlightSqlCommand::StatementQuery(cmd));
        }
        
        Err(Status::invalid_argument("Unknown Flight SQL command"))
    }

    fn extract_sql_from_command(&self, cmd_bytes: &[u8]) -> std::result::Result<String, Status> {
        // First try to handle as Flight SQL command
        match self.handle_flight_sql_command(cmd_bytes) {
            Ok(FlightSqlCommand::StatementQuery(cmd)) => {
                return Ok(cmd.query);
            }
            Ok(FlightSqlCommand::GetSqlInfo(_)) => {
                // This is a metadata request, not a SQL query
                return Err(Status::invalid_argument("CommandGetSqlInfo is a metadata request, not a SQL query"));
            }
            Err(_) => {
                // Fall through to other parsing methods
            }
        }
        
        // Simple heuristic: look for SQL-like content in the bytes
        // This is a temporary solution until we properly integrate the protobuf definitions
        if let Ok(s) = String::from_utf8(cmd_bytes.to_vec()) {
            // Look for common SQL keywords
            let s_lower = s.to_lowercase();
            if s_lower.contains("select") || s_lower.contains("show") || s_lower.contains("describe") {
                // Try to extract the actual SQL part
                if let Some(start) = s.find("SELECT") {
                    return Ok(s[start..].trim().to_string());
                } else if let Some(start) = s.find("select") {
                    return Ok(s[start..].trim().to_string());
                } else if let Some(start) = s.find("SHOW") {
                    return Ok(s[start..].trim().to_string());
                } else if let Some(start) = s.find("show") {
                    return Ok(s[start..].trim().to_string());
                }
                // If we can't find a clear start, return the whole string
                return Ok(s);
            }
        }
        
        // Fallback: try to decode as Spark Connect SqlCommand
        if let Ok(cmd) = SqlCommand::decode(cmd_bytes) {
            if let Some(input) = &cmd.input {
                if let Some(rel_type) = &input.rel_type {
                    match rel_type {
                        sail_spark_connect::spark::connect::relation::RelType::Sql(sql_rel) => {
                            return Ok(sql_rel.query.clone());
                        }
                        _ => {}
                    }
                }
            }
            #[allow(deprecated)]
            return Ok(cmd.sql);
        }
        
        Err(Status::invalid_argument("Could not extract SQL from command"))
    }

    async fn handle_get_sql_info(&self, _cmd: CommandGetSqlInfo) -> std::result::Result<Vec<FlightData>, Status> {
        println!("* Handling GetSqlInfo request - returning server metadata");
        
        // Create schema for SqlInfo response
        // This should match the schema defined in the Flight SQL spec
        let schema = arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("info_name", arrow::datatypes::DataType::UInt32, false),
            arrow::datatypes::Field::new("value", arrow::datatypes::DataType::Utf8, true),
        ]);

        // Create some basic server info
        let info_names = arrow::array::UInt32Array::from(vec![
            SqlInfo::FlightSqlServerName as u32,
            SqlInfo::FlightSqlServerVersion as u32,
            SqlInfo::FlightSqlServerReadOnly as u32,
            SqlInfo::FlightSqlServerSql as u32,
        ]);
        
        let values = arrow::array::StringArray::from(vec![
            Some("Sail Flight SQL Server"),
            Some(env!("CARGO_PKG_VERSION")),
            Some("false"), // Not read-only
            Some("true"),  // Supports SQL
        ]);

        let record_batch = arrow::record_batch::RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(info_names), Arc::new(values)]
        ).map_err(|e| Status::internal(format!("Failed to create SqlInfo record batch: {}", e)))?;

        // Convert to FlightData
        let flight_data = self.record_batch_to_flight_data(record_batch)?;
        Ok(vec![flight_data])
    }
    
    async fn execute_demo_query(&self) -> std::result::Result<Vec<FlightData>, Status> {
        println!("* Executing demo query...");
        
        // Create a DataFusion session context for SQL execution
        let ctx = SessionContext::new();
        println!("* Created SessionContext");
        
        // Add some demo data to the context
        self.add_demo_data(&ctx).await?;
        println!("* Added demo data to context");
        
        // Execute a simple demo query
        let query = "SELECT * FROM demo_table";
        println!("* Executing SQL: {}", query);
        
        match ctx.sql(query).await {
            Ok(df) => {
                println!("* SQL query parsed successfully");
                
                // Convert DataFrame to RecordBatch stream
                let stream = df.execute_stream().await
                    .map_err(|e| Status::internal(format!("Failed to execute query: {}", e)))?;
                println!("* Created RecordBatch stream");
                
                // Convert RecordBatch stream to FlightData stream
                let flight_data_stream = self.convert_to_flight_data(stream)?;
                println!("* Created FlightData stream");
                
                // Collect the stream into a vector for now
                let mut flight_data = Vec::new();
                tokio::pin!(flight_data_stream);
                
                println!("* Reading from FlightData stream...");
                while let Some(data) = flight_data_stream.next().await {
                    match data {
                        Ok(data) => {
                            flight_data.push(data);
                            println!("* Added FlightData item, total: {}", flight_data.len());
                        },
                        Err(e) => {
                            println!("* Stream error: {}", e);
                            return Err(Status::internal(format!("Stream error: {}", e)));
                        }
                    }
                }
                
                println!("* Demo query executed successfully - {} FlightData items", flight_data.len());
                Ok(flight_data)
            }
            Err(e) => {
                println!("* Demo query execution error: {}", e);
                Ok(vec![])
            }
        }
    }
}

#[tonic::async_trait]
impl FlightService for SailFlightService {
    type HandshakeStream = BoxFlightStream<HandshakeResponse>;
    type ListFlightsStream = BoxFlightStream<FlightInfo>;
    type DoGetStream = BoxFlightStream<FlightData>;
    type DoPutStream = BoxFlightStream<PutResult>;
    type DoExchangeStream = BoxFlightStream<FlightData>;
    type DoActionStream = BoxFlightStream<arrow_flight::Result>;
    type ListActionsStream = BoxFlightStream<ActionType>;

    async fn handshake(
        &self,
        _req: Request<Streaming<HandshakeRequest>>,
    ) -> TonicResult<Response<Self::HandshakeStream>> {
        let resp = HandshakeResponse { protocol_version: 0, payload: Default::default() };
        let stream = stream::iter(vec![Ok(resp)]);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn list_flights(
        &self,
        _req: Request<Criteria>,
    ) -> TonicResult<Response<Self::ListFlightsStream>> {
        let stream = stream::iter(Vec::<TonicResult<FlightInfo>>::new());
        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> TonicResult<Response<FlightInfo>> {
        let descriptor = request.into_inner();
        if descriptor.r#type == flight_descriptor::DescriptorType::Cmd as i32 {
            // First try to handle as Flight SQL command
            match self.handle_flight_sql_command(&descriptor.cmd) {
                Ok(FlightSqlCommand::GetSqlInfo(cmd)) => {
                    let rows = self.handle_get_sql_info(cmd).await?;
                    let ticket = "get_sql_info".to_string();
                    self.queries.lock().unwrap().insert(ticket.clone(), rows);
                    let info = FlightInfo {
                        schema: vec![].into(),
                        flight_descriptor: Some(descriptor),
                        endpoint: vec![FlightEndpoint {
                            ticket: Some(Ticket { ticket: ticket.into_bytes().into() }),
                            location: vec![],
                            expiration_time: None,
                            app_metadata: vec![].into(),
                        }],
                        total_records: -1,
                        total_bytes: -1,
                        ordered: false,
                        app_metadata: vec![].into(),
                    };
                    return Ok(Response::new(info));
                }
                Ok(FlightSqlCommand::StatementQuery(cmd)) => {
                    let rows = self.execute_sql_query(cmd.query.clone()).await?;
                    let ticket = format!("sql_query_{}", cmd.query.len());
                    self.queries.lock().unwrap().insert(ticket.clone(), rows);
                    let info = FlightInfo {
                        schema: vec![].into(),
                        flight_descriptor: Some(descriptor),
                        endpoint: vec![FlightEndpoint {
                            ticket: Some(Ticket { ticket: ticket.into_bytes().into() }),
                            location: vec![],
                            expiration_time: None,
                            app_metadata: vec![].into(),
                        }],
                        total_records: -1,
                        total_bytes: -1,
                        ordered: false,
                        app_metadata: vec![].into(),
                    };
                    return Ok(Response::new(info));
                }
                Err(_) => {
                    // Fall back to legacy SQL extraction
                    if let Ok(sql) = self.extract_sql_from_command(&descriptor.cmd) {
                        let rows = self.execute_sql_query(sql.clone()).await?;
                        let ticket = format!("sql_query_{}", sql.len());
                        self.queries.lock().unwrap().insert(ticket.clone(), rows);
                        let info = FlightInfo {
                            schema: vec![].into(),
                            flight_descriptor: Some(descriptor),
                            endpoint: vec![FlightEndpoint {
                                ticket: Some(Ticket { ticket: ticket.into_bytes().into() }),
                                location: vec![],
                                expiration_time: None,
                                app_metadata: vec![].into(),
                            }],
                            total_records: -1,
                            total_bytes: -1,
                            ordered: false,
                            app_metadata: vec![].into(),
                        };
                        return Ok(Response::new(info));
                    }
                }
            }
        }
        Err(Status::unimplemented("get_flight_info not implemented for this descriptor type"))
    }

    async fn poll_flight_info(
        &self,
        _req: Request<FlightDescriptor>,
    ) -> TonicResult<Response<PollInfo>> {
        Err(Status::unimplemented("poll_flight_info not implemented"))
    }

    async fn get_schema(
        &self,
        _req: Request<FlightDescriptor>,
    ) -> TonicResult<Response<SchemaResult>> {
        Err(Status::unimplemented("get_schema not implemented"))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> TonicResult<Response<Self::DoGetStream>> {
        let ticket = String::from_utf8_lossy(&request.into_inner().ticket).to_string();
        if let Some(data) = self.queries.lock().unwrap().get(&ticket) {
            let stream = stream::iter(data.clone().into_iter().map(|x| Ok(x)));
            Ok(Response::new(Box::pin(stream)))
        } else if ticket == "demo_query" {
            let demo = self.execute_demo_query().await?;
            let stream = stream::iter(demo.into_iter().map(|x| Ok(x)));
            Ok(Response::new(Box::pin(stream)))
        } else {
            let stream = stream::iter(Vec::<TonicResult<FlightData>>::new());
            Ok(Response::new(Box::pin(stream)))
        }
    }

    async fn do_put(
        &self,
        _req: Request<Streaming<FlightData>>,
    ) -> TonicResult<Response<Self::DoPutStream>> {
        let stream = stream::iter(Vec::<TonicResult<PutResult>>::new());
        Ok(Response::new(Box::pin(stream)))
    }

    async fn do_exchange(
        &self,
        _req: Request<Streaming<FlightData>>,
    ) -> TonicResult<Response<Self::DoExchangeStream>> {
        let stream = stream::iter(Vec::<TonicResult<FlightData>>::new());
        Ok(Response::new(Box::pin(stream)))
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> TonicResult<Response<Self::DoActionStream>> {
        let action = request.into_inner();
        match action.r#type.as_str() {
            "CreatePreparedStatement" => {/* TODO */},
            "ClosePreparedStatement" => {/* TODO */},
            _ => {}
        }
        let stream = stream::iter(Vec::<TonicResult<arrow_flight::Result>>::new());
        Ok(Response::new(Box::pin(stream)))
    }

    async fn list_actions(
        &self,
        _req: Request<Empty>,
    ) -> TonicResult<Response<Self::ListActionsStream>> {
        let actions = vec![
            ActionType { r#type: "CreatePreparedStatement".into(), description: "Create a prepared statement".into() },
            ActionType { r#type: "ClosePreparedStatement".into(), description: "Close a prepared statement".into() }
        ];
        let stream = stream::iter(actions.into_iter().map(|x| Ok(x)));
        Ok(Response::new(Box::pin(stream)))
    }
}

#[derive(Eq, PartialEq, Hash, Clone, Debug)]
pub struct SessionKey {
    pub user_id: Option<String>,
    pub session_id: String,
}

impl Display for SessionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(user_id) = &self.user_id {
            write!(f, "{}@{}", user_id, self.session_id)
        } else {
            write!(f, "{}", self.session_id)
        }
    }
}

#[derive(Debug, Clone)]
pub struct FlightSessionManagerOptions {
    pub config: Arc<AppConfig>
}

pub struct FlightSessionManagerActor {
    options: FlightSessionManagerOptions,
    system: Arc<Mutex<ActorSystem>>,
    queries: HashMap<String, Vec<FlightData>>,
    sessions: HashMap<SessionKey, SessionContext>,
}

pub enum FlightSessionManagerEvent {
    SqlQuery(String),
    GetOrCreateFlightSession {
        key: SessionKey,
        system: Arc<Mutex<ActorSystem>>,
        result: oneshot::Sender<SparkResult<SessionContext>>,
    },
    ProbeIdleFlightSession {
        key: SessionKey,
        instant: Instant,
    },
    // Retry logic created on client side
    TellQuery {
        query: String,
    },
}

impl Actor for FlightSessionManagerActor {
    type Message = FlightSessionManagerEvent;
    type Options = FlightSessionManagerOptions;

    fn new(options: Self::Options) -> Self {
        Self {
            options,
            system: Arc::new(Mutex::new(ActorSystem::new())),
            queries: HashMap::new(),
            sessions: HashMap::new(),
        }
    }

    fn receive(&mut self, _ctx: &mut ActorContext<Self>, message: Self::Message) -> ActorAction {
        match message {
            FlightSessionManagerEvent::SqlQuery(query) => {
                self.handle_sql_query(query)
            }
            FlightSessionManagerEvent::GetOrCreateFlightSession { key, system, result } => {
                self.handle_get_or_create_session(_ctx, key, system, result)
            }
            FlightSessionManagerEvent::ProbeIdleFlightSession { key, instant } => {
                self.handle_probe_idle_session(_ctx, key, instant)
            }
            FlightSessionManagerEvent::TellQuery { query } => {
                self.queries.insert(query, vec![]);
                ActorAction::Continue
            }
        }
    }
}

impl FlightSessionManagerActor {
    fn handle_sql_query(&mut self, query: String) -> ActorAction {
        println!("* Actor handling SQL query: {}", query);
        // For now, just store the query and return demo data
        self.queries.insert(query, vec![]);
        ActorAction::Continue
    }

    fn handle_get_or_create_session(
        &mut self,
        ctx: &mut ActorContext<Self>,
        key: SessionKey,
        system: Arc<Mutex<ActorSystem>>,
        result: oneshot::Sender<SparkResult<SessionContext>>,
    ) -> ActorAction {
        let entry = self.sessions.entry(key.clone());
        let context = match entry {
            Entry::Occupied(o) => Ok(o.get().clone()),
            Entry::Vacant(v) => {
                let key = v.key().clone();
                info!("creating session {key}");
                // For now, create a simple session context
                // TODO: Implement proper session creation
                let context = SessionContext::new();
                Ok(v.insert(context).clone())
            }
        };
        if let Ok(context) = &context {
            if let Ok(active_at) =
                SparkExtension::get(context).and_then(|spark| spark.track_activity())
            {
                ctx.send_with_delay(
                    FlightSessionManagerEvent::ProbeIdleFlightSession {
                        key,
                        instant: active_at,
                    },
                    Duration::from_secs(self.options.config.spark.session_timeout_secs),
                );
            }
        }
        let _ = result.send(context);
        ActorAction::Continue
    }

    fn handle_probe_idle_session(
        &mut self,
        ctx: &mut ActorContext<Self>,
        key: SessionKey,
        instant: Instant,
    ) -> ActorAction {
        let context = self.sessions.get(&key);
        if let Some(context) = context {
            if let Ok(spark) = SparkExtension::get(context) {
                if spark.active_at().is_ok_and(|x| x <= instant.into()) {
                    info!("removing idle session {key}");
                    ctx.spawn(async move { spark.job_runner().stop().await });
                    self.sessions.remove(&key);
                }
            }
        }
        ActorAction::Continue
    }
}

pub struct FlightSessionManager {
    system: Arc<Mutex<ActorSystem>>,
    handle: ActorHandle<FlightSessionManagerActor>,
}

impl Debug for FlightSessionManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlightSessionManager").finish()
    }
}

impl FlightSessionManager {
    pub fn new(options: FlightSessionManagerOptions) -> Self {
        let mut system = ActorSystem::new();
        let handle = system.spawn::<FlightSessionManagerActor>(options);
        Self {
            system: Arc::new(Mutex::new(system)),
            handle,
        }
    }

    pub async fn get_or_create_session_context(
        &self,
        key: SessionKey,
    ) -> SparkResult<SessionContext> {
        let (tx, rx) = oneshot::channel();
        let event = FlightSessionManagerEvent::GetOrCreateFlightSession {
            key,
            system: self.system.clone(),
            result: tx,
        };
        self.handle.send(event).await?;
        rx.await
            .map_err(|e| SparkError::internal(format!("failed to get session: {e}")))?
    }

    // pub fn create_session_context(
    //     system: Arc<Mutex<ActorSystem>>,
    //     key: SessionKey,
    //     options: FlightSessionManagerOptions,
    // ) -> SparkResult<SessionContext> {
    //     let job_runner: Box<dyn JobRunner> = match options.config.mode {
    //         ExecutionMode::Local => Box::new(LocalJobRunner::new()),
    //         ExecutionMode::LocalCluster | ExecutionMode::KubernetesCluster => {
    //             let options = DriverOptions::try_new(&options.config, options.runtime.clone())?;
    //             let mut system = system.lock()?;
    //             Box::new(ClusterJobRunner::new(system.deref_mut(), options))
    //         }
    //     };
    //     // TODO: support more systematic configuration
    //     // TODO: return error on invalid environment variables
    //     let mut session_config = SessionConfig::new()
    //         .with_create_default_catalog_and_schema(true)
    //         .with_default_catalog_and_schema(DEFAULT_SPARK_CATALOG, DEFAULT_SPARK_SCHEMA)
    //         // We do not use the information schema since we use the catalog/schema/table providers
    //         // directly for catalog operations.
    //         .with_information_schema(false)
    //         .with_extension(Arc::new(TemporaryViewManager::default()))
    //         .with_extension(Arc::new(SparkExtension::try_new(
    //             key.user_id,
    //             key.session_id,
    //             job_runner,
    //         )?));

    //     // execution options
    //     {
    //         let execution = &mut session_config.options_mut().execution;

    //         execution.batch_size = options.config.execution.batch_size;
    //         execution.collect_statistics = options.config.execution.collect_statistics;
    //         execution.listing_table_ignore_subdirectory = false;
    //     }

    //     // execution Parquet options
    //     {
    //         let parquet = &mut session_config.options_mut().execution.parquet;

    //         parquet.created_by = concat!("sail version ", env!("CARGO_PKG_VERSION")).into();
    //         parquet.enable_page_index = options.config.parquet.enable_page_index;
    //         parquet.pruning = options.config.parquet.pruning;
    //         parquet.skip_metadata = options.config.parquet.skip_metadata;
    //         parquet.metadata_size_hint = options.config.parquet.metadata_size_hint;
    //         parquet.pushdown_filters = options.config.parquet.pushdown_filters;
    //         parquet.reorder_filters = options.config.parquet.reorder_filters;
    //         parquet.schema_force_view_types = options.config.parquet.schema_force_view_types;
    //         parquet.binary_as_string = options.config.parquet.binary_as_string;
    //         parquet.coerce_int96 = Some("us".to_string());
    //         parquet.data_pagesize_limit = options.config.parquet.data_page_size_limit;
    //         parquet.write_batch_size = options.config.parquet.write_batch_size;
    //         parquet.writer_version = options.config.parquet.writer_version.clone();
    //         parquet.skip_arrow_metadata = options.config.parquet.skip_arrow_metadata;
    //         parquet.compression = Some(options.config.parquet.compression.clone());
    //         parquet.dictionary_enabled = Some(options.config.parquet.dictionary_enabled);
    //         parquet.dictionary_page_size_limit = options.config.parquet.dictionary_page_size_limit;
    //         parquet.statistics_enabled = Some(options.config.parquet.statistics_enabled.clone());
    //         parquet.max_row_group_size = options.config.parquet.max_row_group_size;
    //         parquet.column_index_truncate_length =
    //             options.config.parquet.column_index_truncate_length;
    //         parquet.statistics_truncate_length = options.config.parquet.statistics_truncate_length;
    //         parquet.data_page_row_count_limit = options.config.parquet.data_page_row_count_limit;
    //         parquet.encoding = options.config.parquet.encoding.clone();
    //         parquet.bloom_filter_on_read = options.config.parquet.bloom_filter_on_read;
    //         parquet.bloom_filter_on_write = options.config.parquet.bloom_filter_on_write;
    //         parquet.bloom_filter_fpp = Some(options.config.parquet.bloom_filter_fpp);
    //         parquet.bloom_filter_ndv = Some(options.config.parquet.bloom_filter_ndv);
    //         parquet.allow_single_file_parallelism =
    //             options.config.parquet.allow_single_file_parallelism;
    //         parquet.maximum_parallel_row_group_writers =
    //             options.config.parquet.maximum_parallel_row_group_writers;
    //         parquet.maximum_buffered_record_batches_per_stream = options
    //             .config
    //             .parquet
    //             .maximum_buffered_record_batches_per_stream;
    //     }

    //     let runtime = {
    //         let registry = DynamicObjectStoreRegistry::new(options.runtime.clone());
    //         let builder =
    //             RuntimeEnvBuilder::default().with_object_store_registry(Arc::new(registry));
    //         Arc::new(builder.build()?)
    //     };
    //     let state = SessionStateBuilder::new()
    //         .with_config(session_config)
    //         .with_runtime_env(runtime)
    //         .with_default_features()
    //         .with_analyzer_rules(default_analyzer_rules())
    //         .with_optimizer_rules(default_optimizer_rules())
    //         .with_query_planner(new_query_planner())
    //         .build();
    //     let context = SessionContext::new_with_state(state);

    //     // TODO: This is a temp workaround to deregister all built-in functions that we define.
    //     //   We should deregister all context.udfs() once we have better coverage of functions.
    //     //   handler.rs needs to do this
    //     for (&name, _function) in BUILT_IN_SCALAR_FUNCTIONS.iter() {
    //         context.deregister_udf(name);
    //     }
    //     for (&name, _function) in BUILT_IN_GENERATOR_FUNCTIONS.iter() {
    //         context.deregister_udf(name);
    //     }
    //     for (&name, _function) in BUILT_IN_TABLE_FUNCTIONS.iter() {
    //         context.deregister_udtf(name);
    //     }

    //     Ok(context)
    // }
}