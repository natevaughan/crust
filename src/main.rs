use clap::Parser;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::{ClientConfig, Message};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct CliArgs {
    /// Kafka server instances
    #[arg(short, long)]
    bootstrap_servers: Option<String>,

    /// Kafka message topic name instances
    #[arg(short, long)]
    message_topic_name: Option<String>,
}

#[tokio::main]
async fn main() {
    let mut stdout = tokio::io::stdout();
    let args = CliArgs::parse();
    stdout.write(b"Initializing crust chat.\n").await.unwrap();
    let bootstrap_servers = &args.bootstrap_servers.unwrap_or("localhost".to_string());
    let producer = init_producer(&bootstrap_servers);

    let mut input_lines = BufReader::new(tokio::io::stdin()).lines();
    let topic_name = &args.message_topic_name.unwrap_or("messages".to_string());
    let consumer = create_consumer(&bootstrap_servers);

    consumer.subscribe(&[&topic_name]).unwrap();

    loop {
        stdout.write(b"> ").await.unwrap();
        stdout.flush().await.unwrap();

        tokio::select! {
            message = consumer.recv() => {
                let message  = message.expect("Failed to read message").detach();
                let payload = message.payload().unwrap();
                stdout.write(payload).await.unwrap();
                stdout.write(b"\n").await.unwrap();
            }
            line = input_lines.next_line() => {
                match line {
                    Ok(Some(line)) => {
                        producer.send(FutureRecord::<(), _>::to("chat")
                          .payload(&line), Timeout::Never)
                            .await
                            .expect("Failed to produce");
                    }
                    _ => break,
                }
            }
        }
    }
}

fn init_producer(bootstrap_server: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap_server)
        .set("queue.buffering.max.ms", "0")
        .create()
        .expect("Failed to create client")
}

fn create_consumer(bootstrap_server: &str) -> StreamConsumer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap_server)
        .set("enable.partition.eof", "false")
        // We'll give each session its own (unique) consumer group id,
        // so that each session will receive all messages
        .set("group.id", format!("chat-{}", Uuid::new_v4()))
        .create()
        .expect("Failed to create client")
}
