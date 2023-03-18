use blackbird_macros::struct_for;

struct_for!(Person, "./blackbird/examples/migrations");

#[test]
fn test_can_use_struct() {
    let p = Person {
        name: Some("john".to_string()),
        username: "big_j".to_string(),
    };

    assert_eq!(p.name.unwrap(), "john");
}
