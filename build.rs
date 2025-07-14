fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protobuf_include = "third_party/protobuf/src";
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir("src/generated") // Optional: where to put generated .rs files
        .compile_protos(
            &[
                "src/proto/sail/Flight.proto",
                "src/proto/sail/FlightSql.proto",
            ],
            &["src/proto", protobuf_include], // Your own .proto root
        )?;

    Ok(())
}

