use std::env;

#[macro_export]
macro_rules! assert_snapshot {
    ($($arg:tt)*) => {
        $crate::test_utils::insta::settings().bind(|| {
            insta::assert_snapshot!($($arg)*);
        })
    };
}

pub fn settings() -> insta::Settings {
    let mut settings = insta::Settings::clone_current();

    // Filter references to the working directory
    let cwd = env::current_dir().unwrap();
    let dir = cwd.to_str().unwrap();
    settings.add_filter(dir.trim_start_matches("/"), "");
    let dir = cwd.parent().unwrap().to_str().unwrap();
    settings.add_filter(dir.trim_start_matches("/"), "");

    settings
}
