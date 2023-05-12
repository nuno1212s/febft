pub type ArgsFunc<T> = Box<dyn FnOnce(&mut T)>;

pub type DefaultConfig<T> = fn() -> T;

pub fn initialize_config<T>(default: DefaultConfig<T>, alterations: Vec<ArgsFunc<T>>) -> T {
    let mut config = default();

    for alter in alterations {
        alter(&mut config);
    }

    config
}