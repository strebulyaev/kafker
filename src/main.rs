use std::time::{Duration, SystemTime};
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use openssl_probe::init_ssl_cert_env_vars;
use std::env;
use std::ffi::OsString;
use kafka::client::KafkaClient;
use pico_args::Arguments;

enum Command {
    List(String),
    Listen(String, Vec<String>)
}

fn get_host(arguments: &mut Arguments) -> String {
    arguments.value_from_str(["-h", "--host"]).unwrap_or("127.0.0.1:9092".to_string())
}

fn get_topics(arguments: &mut Arguments) -> Vec<String> {
    arguments.values_from_str(["-t", "--topics"]).unwrap()
}

fn create_consumer(host: String, topics: Vec<String>) -> kafka::Result<Consumer> {
    // Create a new Kafka consumer
    let mut builder = Consumer::from_hosts(vec![host])
        .with_group("kafker-group".to_string())
        .with_fallback_offset(FetchOffset::Earliest)
        .with_offset_storage(Option::from(GroupOffsetStorage::Kafka));

    for topic in topics {
        builder = builder.with_topic(topic);
    }

    builder.create()
}

fn list_topics(host: String) {

    let mut client = KafkaClient::new(vec!(host));

    match client.load_metadata_all() {
        Ok(_) => {
            let topics = client.topics();
            println!("Total topics: {}", topics.len());
            for t in topics {
                println!("{}", t.name())
            }
        },
        Err(e) => println!("Error loading metadata: {}", e),
    }
}

fn listen(host: String, topics: Vec<String>) {
    let mut consumer = match create_consumer(host, topics) {
        Ok(consumer) => consumer,
        Err(e) => {
            eprintln!("Failed to create consumer: {}", e);
            return;
        }
    };

    // Consume messages in an infinite loop
    loop {
        // Poll messages from Kafka
        let messages = consumer.poll().unwrap();

        // Print each message to the console
        for message_set in messages.iter() {
            for message in message_set.messages() {
                println!("{} {}\n{}",
                         humantime::format_rfc3339_millis(SystemTime::now()),
                         message_set.topic(),
                         String::from_utf8_lossy(message.value));
            }

            // Commit the offset of the last message consumed
            let _ = consumer.consume_messageset(message_set);
        }

        // Commit the offsets to Kafka
        consumer.commit_consumed().unwrap();

        // Sleep for a short duration before polling again
        std::thread::sleep(Duration::from_millis(1000));
    }
}


fn main() {
    // Ensure that OpenSSL can find the system certs
    init_ssl_cert_env_vars();

    let args: Vec<OsString> = env::args_os().collect();
    if args.len() < 2 {
        let my_string = include_str!("usage.txt");
        eprintln!("{}", my_string);
        return;
    }

    let mut arguments = Arguments::from_vec(args[1..].to_owned());

    let command: Command = match args[1].clone().into_string() {
        Ok(arg) => match arg.as_str() {
            "list" => {
                Command::List(get_host(&mut arguments))
            },
            "listen" => {
                let host = get_host(&mut arguments);
                let topics: Vec<String> = get_topics(&mut arguments);
                Command::Listen(host, topics)
            },
            _ => {
                eprintln!("Unsupported command. Please use either 'list' or 'listen'.");
                return;
            }
        },
        Err(err) => {
            eprintln!("Failed to parse arguments: {:?}", err);
            return;
        }
    };

    match command {
        Command::List(host) => list_topics(host),
        Command::Listen(host, topics) => listen(host, topics),
    }
}
