use gateway_core::{engine::budget::*, metering::*};

fn money(amount: &str) -> Money {
    Money::new(amount.parse().unwrap(), CurrencyCode::new("USD").unwrap())
}
fn breakdown() -> CalculatedCostBreakdown {
    CalculatedCostBreakdown::new(
        CalculatedCostAmounts::new(
            money("2"),
            money("3"),
            money("1"),
            money("1"),
            money("12"),
            money("12"),
        ),
        CalculatedCostRates::new(money("0"), money("0"), money("0"), money("0")),
        Some("default".into()),
        100,
    )
    .with_image(ImageCostBreakdown {
        input_tokens: 10,
        cached_tokens: 1,
        input_amount: money("1"),
        cache_read_amount: money("1"),
        input_price_per_million: money("0"),
        cache_read_price_per_million: money("0"),
    })
}
#[test]
fn component_budget_facts_include_native_image_fees_without_guessing_other_fees() {
    let cost = breakdown().calculated_cost().into_estimate();
    assert_eq!(
        budget_components(&cost).map(Decimal::canonical),
        ["3", "3", "2", "1"]
    );
    assert_eq!(cost.total().unwrap().amount().canonical(), "12");
}
#[test]
fn unitemized_and_non_usd_costs_do_not_acquire_component_fees() {
    let unknown = CalculatedCost::from_usd_ticks(100).unwrap().into_estimate();
    assert_eq!(budget_components(&unknown), [Decimal::ZERO; 4]);
    let non_usd = CalculatedCostBreakdown::new(
        CalculatedCostAmounts::new(
            money("2"),
            money("3"),
            money("1"),
            money("1"),
            money("12"),
            Money::new("12".parse().unwrap(), CurrencyCode::new("EUR").unwrap()),
        ),
        CalculatedCostRates::new(money("0"), money("0"), money("0"), money("0")),
        None,
        100,
    )
    .calculated_cost()
    .into_estimate();
    assert_eq!(budget_components(&non_usd), [Decimal::ZERO; 4]);
}
#[test]
fn admission_billing_accepts_zero_and_rejects_out_of_range_rules() {
    assert_eq!(ClientBudgetBilling::default().multiplier().canonical(), "1");
    assert!(ClientBudgetBilling::new(Decimal::ZERO, MultiplierScope::All).is_ok());
    assert!(
        ClientBudgetBilling::new("1000.0000000001".parse().unwrap(), MultiplierScope::All).is_err()
    );
    assert!(
        ClientBudgetBilling::new(
            "1".parse().unwrap(),
            MultiplierScope::Components(vec![].into_iter().collect())
        )
        .is_err()
    );
}
