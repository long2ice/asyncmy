use mysql_async::prelude::*;
use std::future::Future;
use std::time::Instant;

type Mixed = (
    i64,
    i64,
    i64,
    f64,
    String,
    String,
    String,
    time::PrimitiveDateTime,
    time::Date,
    time::Duration,
);
type Integers = (i64, i64, i64, i64, i64, i64, i64, i64);
enum Value {
    Integers(Vec<Integers>),
    Mixed(Vec<Mixed>),
    Point(i64),
}
async fn measure<F, Fut>(name: &str, mut f: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Value>,
{
    f().await;
    let mut samples = Vec::new();
    for _ in 0..7 {
        let start = Instant::now();
        let result = f().await;
        samples.push(start.elapsed().as_secs_f64() * 1000.0);
        let sum = match result {
            Value::Integers(rows) => {
                assert_eq!(rows.len(), 50000);
                rows.iter().map(|r| r.0).sum()
            }
            Value::Mixed(rows) => {
                assert_eq!(rows.len(), 50000);
                rows.iter().map(|r| r.1).sum()
            }
            Value::Point(sum) => sum,
        };
        assert_eq!(
            sum,
            if name.starts_with("scan_") {
                1249975000
            } else {
                0
            }
        );
    }
    let mut sorted = samples.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    println!(
        "{{\"runtime\":\"rust\",\"case\":\"{name}\",\"median_ms\":{},\"samples_ms\":{:?}}}",
        sorted[3], samples
    );
}
async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let url="mysql://root:123456@127.0.0.1:3306/test?pool_min=10&pool_max=10&prefer_socket=false&reset_connection=false";
    let pool = mysql_async::Pool::new(url);
    let mut held = Vec::new();
    for _ in 0..10 {
        let mut conn = pool.get_conn().await?;
        conn.query_drop("SET autocommit=1").await?;
        held.push(conn);
    }
    drop(held);
    for kind in ["integer", "mixed"] {
        let query = if kind == "integer" {
            "SELECT a,b,a,b,a,b,a,b FROM test.bench"
        } else {
            "SELECT * FROM test.bench"
        };
        for mode in ["text", "binary"] {
            let mut conn = pool.get_conn().await?;
            let stmt = if mode == "binary" {
                Some(conn.prep(query).await?)
            } else {
                None
            };
            // A mutex only satisfies the async closure lifetime; no contention occurs in scans.
            let conn = std::sync::Arc::new(tokio::sync::Mutex::new(conn));
            measure(&format!("scan_{kind}_{mode}"), || {
                let conn = conn.clone();
                let stmt = stmt.clone();
                async move {
                    let mut conn = conn.lock().await;
                    if kind == "integer" {
                        let rows: Vec<Integers> = if let Some(stmt) = stmt {
                            conn.exec(stmt, ()).await.unwrap()
                        } else {
                            conn.query(query).await.unwrap()
                        };
                        Value::Integers(rows)
                    } else {
                        let rows: Vec<Mixed> = if let Some(stmt) = stmt {
                            conn.exec(stmt, ()).await.unwrap()
                        } else {
                            conn.query(query).await.unwrap()
                        };
                        Value::Mixed(rows)
                    }
                }
            })
            .await;
        }
    }
    for mode in ["text", "binary"] {
        measure(&format!("point_{mode}"), || {
            let pool = pool.clone();
            async move {
                let mut handles = Vec::new();
                for _ in 0..20 {
                    let pool = pool.clone();
                    handles.push(tokio::spawn(async move {
                        let mut sum = 0;
                        for _ in 0..200 {
                            let mut conn = pool.get_conn().await.unwrap();
                            let value: Option<i64> = if mode == "binary" {
                                conn.exec_first("SELECT a FROM test.bench WHERE id=?", (1,))
                                    .await
                                    .unwrap()
                            } else {
                                conn.query_first(format!(
                                    "SELECT a FROM test.bench WHERE id={}",
                                    std::hint::black_box(1)
                                ))
                                .await
                                .unwrap()
                            };
                            sum += value.unwrap();
                        }
                        sum
                    }));
                }
                let mut sum = 0;
                for handle in handles {
                    sum += handle.await.unwrap()
                }
                Value::Point(sum)
            }
        })
        .await;
    }
    pool.disconnect().await?;
    Ok(())
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(run())
}
