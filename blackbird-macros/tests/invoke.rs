use blackbird_macros::struct_for;

struct_for!(Person, "./blackbird/examples/migrations");

#[test]
fn test_can_use_struct() {
    let p = Person {
        name: "john".to_string(),
    };

    assert_eq!(p.name, "john");
}
