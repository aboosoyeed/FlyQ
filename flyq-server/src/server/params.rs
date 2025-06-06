use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "flyq-server")]
pub struct Params {
    #[arg(long, env = "FLYQ_BASE_DIR", default_value = "./data")]
    pub base_dir: String,

    #[arg(long, env = "FLYQ_PORT", default_value_t = 9092)]
    pub port: u16,
}

