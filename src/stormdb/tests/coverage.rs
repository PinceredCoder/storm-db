#[ignore]
#[test]
fn get_tests_coverage() {
    let target_dir = std::env::current_exe()
        .ok()
        .and_then(|path| {
            path.parent() // remove executable name
                .and_then(|p| p.parent()) // remove 'deps'
                .and_then(|p| p.parent()) // remove 'debug' or 'release'
                .map(|p| p.to_path_buf())
        })
        .unwrap();

    let output = std::process::Command::new("cargo")
        .arg("llvm-cov")
        .arg("--lcov")
        .arg("--output-path")
        .arg(format!("{}/lcov.info", target_dir.display()))
        .output()
        .expect("failed to execute process");

    println!("STDOUT:\n{}", String::from_utf8(output.stdout).unwrap());
    println!("STDERR:\n{}", String::from_utf8(output.stderr).unwrap());
}
