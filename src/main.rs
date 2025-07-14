mod connect;
mod generated;

use clap::{Parser, Subcommand};
use connect::SailFlightService;
use tonic::transport::Server;
use arrow_flight::flight_service_server::FlightServiceServer;

#[derive(Parser)]
#[command(name = "sail")]
#[command(about = "Sail Arrow Flight SQL Server")]
#[command(version = "0.1.0")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the Arrow Flight SQL server
    Flight {
        #[command(subcommand)]
        flight_command: FlightCommands,
    },
}

#[derive(Subcommand)]
enum FlightCommands {
    /// Start the Flight SQL server
    Server {
        /// Port to listen on
        #[arg(short, long, default_value = "32010")]
        port: u16,
        
        /// Host to bind to
        #[arg(long, default_value = "127.0.0.1")]
        host: String,
        
        /// Enable verbose logging
        #[arg(short, long)]
        verbose: bool,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Flight { flight_command } => {
            match flight_command {
                FlightCommands::Server { port, host, verbose } => {
                    run_flight_server(host, *port, *verbose).await?;
                }
            }
        }
    }

    Ok(())
}

async fn run_flight_server(host: &str, port: u16, verbose: bool) -> Result<(), Box<dyn std::error::Error>> {
    if verbose {
        println!("🔧 Starting Sail Arrow Flight SQL Server in verbose mode...");
    } else {
        println!("🚀 Starting Sail Arrow Flight SQL Server...");
    }
    
    let addr = format!("{}:{}", host, port).parse()?;
    let service = SailFlightService::new();
    
    println!("📍 Server listening on {}", addr);
    println!("🔗 JDBC clients can connect to: jdbc:arrow-flight-sql://{}:{}", host, port);
    
    if verbose {
        println!("📊 Verbose logging enabled");
        println!("🔧 Server configuration:");
        println!("   - Host: {}", host);
        println!("   - Port: {}", port);
        println!("   - Protocol: Arrow Flight SQL over gRPC");
    }
    
    Server::builder()
        .add_service(FlightServiceServer::new(service))
        .serve(addr)
        .await?;
    
    Ok(())
}
