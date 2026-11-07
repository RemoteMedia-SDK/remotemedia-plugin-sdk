//! Deliberately stalled interpreter with detached descendants, selected only by tests.
use std::{
    fs::File,
    io::Write,
    process::Command,
    time::{Duration, Instant},
};

fn main() {
    let exe = std::env::current_exe().unwrap();
    let root = exe.parent().unwrap();
    let args: Vec<String> = std::env::args().collect();
    let depth: u8 = if args.get(1).map(String::as_str) == Some("descendant") {
        args[2].parse().unwrap()
    } else {
        0
    };
    #[cfg(target_os = "linux")]
    if depth > 0 {
        extern "C" {
            fn setsid() -> i32;
        }
        assert!(unsafe { setsid() } >= 0);
    }
    let lock = File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(root.join(format!("lifetime-{depth}")))
        .unwrap();
    fs2::FileExt::lock_exclusive(&lock).unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    if depth < 2 {
        let mut child = Command::new(&exe)
            .args(["descendant", &(depth + 1).to_string()])
            .spawn()
            .unwrap();
        while !root.join(format!("ready-{}", depth + 1)).exists() {
            assert!(Instant::now() < deadline);
            assert!(child.try_wait().unwrap().is_none());
            std::thread::sleep(Duration::from_millis(5));
        }
    }
    std::fs::write(
        root.join(format!("ready-{depth}")),
        std::process::id().to_string(),
    )
    .unwrap();
    let behavior = std::fs::read_to_string(root.join("behavior")).unwrap();
    if depth == 0 && behavior != "timeout" {
        let pid = std::process::id() + u32::from(behavior == "wrong-pid");
        println!("{}", serde_json::json!({"base_executable":exe, "pid":pid}));
        std::io::stdout().flush().unwrap();
        return;
    }
    while Instant::now() < deadline {
        if root.join("after-return").exists() {
            std::fs::write(root.join(format!("late-{depth}")), b"still running").unwrap();
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    drop(lock);
}
