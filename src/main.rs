use tokio::io::AsyncWriteExt;

#[tokio::main]
async fn main() {
    let mut stdout = tokio::io::stdout();
    stdout.write(b"Crust chat initialized.\n").await.unwrap();
}
