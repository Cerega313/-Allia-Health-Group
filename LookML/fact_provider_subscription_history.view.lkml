view: fact_provider_subscription_history {
  sql_table_name: `data_mart.fact_provider_subscription_history` ;;

  ########################################################
  ## Dimensions
  ########################################################

  dimension: provider_id {
    label: "Provider ID"
    primary_key: yes
    type: string
    sql: ${TABLE}.provider_id ;;
  }

  dimension: provider_name {
    label: "Provider name"
    type: string
    sql: ${TABLE}.provider_name ;;
  }

  dimension: npi {
    label: "NPI"
    type: string
    sql: ${TABLE}.npi ;;
  }

  dimension: state {
    label: "State"
    type: string
    sql: ${TABLE}.state ;;
  }

  dimension: cycle_length_months {
    label: "Cycle length (months)"
    type: number
    sql: ${TABLE}.cycle_length_months ;;
  }
  dimension_group: period_month {
    label: "Period"
    type: time
    timeframes: [raw, date, month, month_name, year]
    convert_tz: no
    sql: ${TABLE}.period_month ;;
  }

  dimension: subscription_event_type {
    label: "Subscription event type"
    description: "NEW / RENEWAL / REACTIVATED / FROZEN / EXPIRED / CANCELLED / EXPIRING_THIS_MONTH / UP_FOR_RENEWAL"
    type: string
    sql: ${TABLE}.subscription_event_type ;;
  }

  dimension: subscription_status {
    label: "Current subscription status"
    description: "ACTIVE / FROZEN / EXPIRED / CANCELLED"
    type: string
    sql: ${TABLE}.subscription_status ;;
  }

  dimension: churn_label {
    label: "Churn label"
    description: "CHURNED / ACTIVE / AT_RISK"
    type: string
    sql: ${TABLE}.churn_label ;;
  }

  dimension: churn_probability {
    label: "Churn probability"
    type: number
    value_format_name: "percent_1"
    sql: ${TABLE}.churn_probability ;;
  }

  dimension: churn_probability_band {
    label: "Churn probability band"
    description: "<15%, 15–35%, 35–60%, >60%"
    type: string
    sql: ${TABLE}.churn_probability_band ;;
  }

  dimension: is_in_grace_period {
    label: "In grace period"
    type: yesno
    sql: ${TABLE}.is_in_grace_period ;;
  }

  dimension: days_to_cycle_end {
    label: "Days to cycle end"
    type: number
    sql: ${TABLE}.days_to_cycle_end ;;
  }

  dimension: current_cycle_start_date {
    label: "Current cycle start"
    type: date
    sql: ${TABLE}.current_cycle_start_date ;;
  }

  dimension: current_cycle_end_date {
    label: "Current cycle end"
    type: date
    sql: ${TABLE}.current_cycle_end_date ;;
  }

  dimension: cycle_revenue {
    label: "Revenue in current cycle"
    type: number
    sql: ${TABLE}.cycle_revenue ;;
  }

  dimension: lifetime_revenue {
    label: "Lifetime revenue"
    type: number
    sql: ${TABLE}.lifetime_revenue ;;
  }

  dimension: lost_profit_expected {
    label: "Expected lost profit"
    description: "Expected profit loss if provider churns"
    type: number
    sql: ${TABLE}.lost_profit_expected ;;
  }

  ########################################################
  ## Base measures
  ########################################################

  measure: providers_count {
    label: "Providers"
    type: count_distinct
    sql: ${provider_id} ;;
  }

  measure: records_count {
    label: "Rows"
    type: count ;;
  }

  measure: total_lost_profit {
    label: "Lost profit"
    type: sum
    sql: ${lost_profit_expected} ;;
    value_format_name: "usd"
  }

  measure: total_cycle_revenue {
    label: "Cycle revenue"
    type: sum
    sql: ${cycle_revenue} ;;
    value_format_name: "usd"
  }

  ########################################################
  ## KPI measures (Overview)
  ########################################################

  measure: new_subscriptions_count {
    label: "New subscriptions"
    type: count
    filters: [subscription_event_type: "NEW"]
  }

  measure: renewals_count {
    label: "Renewals"
    type: count
    filters: [subscription_event_type: "RENEWAL"]
  }

  measure: reactivated_subscriptions_count {
    label: "Reactivated subscriptions"
    type: count
    filters: [subscription_event_type: "REACTIVATED"]
  }

  measure: frozen_subscriptions_count {
    label: "Frozen subscriptions"
    type: count
    filters: [subscription_status: "FROZEN"]
  }

  measure: up_for_renewal_count {
    label: "Up for renewal"
    type: count
    filters: [subscription_event_type: "UP_FOR_RENEWAL"]
  }

  measure: expiring_this_month_count {
    label: "Expiring this month"
    type: count
    filters: [subscription_event_type: "EXPIRING_THIS_MONTH"]
  }

  measure: churned_providers_count {
    label: "Churned providers"
    type: count
    filters: [churn_label: "CHURNED"]
  }

  measure: overall_churn_rate {
    label: "Churn rate"
    type: number
    value_format_name: "percent_1"
    sql: ${churned_providers_count} / NULLIF(${providers_count}, 0) ;;
  }

  ########################################################
  ## Risk measures (Risks page)
  ########################################################

  measure: providers_in_grace_period_count {
    label: "In grace period"
    type: count
    filters: [is_in_grace_period: "yes"]
  }

  measure: high_risk_providers_count {
    label: "High-risk providers (>60%)"
    type: count
    filters: [churn_probability_band: ">60%"]
  }

  measure: high_risk_lost_profit {
    label: "Potential lost profit (high risk)"
    type: sum
    filters: [churn_probability_band: ">60%"]
    sql: ${lost_profit_expected} ;;
    value_format_name: "usd"
  }

  measure: avg_churn_probability {
    label: "Avg churn probability"
    type: average
    value_format_name: "percent_1"
    sql: ${churn_probability} ;;
  }

  measure: providers_by_band {
    label: "Providers by risk band"
    type: count
  }

  measure: high_risk_lost_profit_by_cycle {
    label: "High-risk lost profit (by cycle)"
    type: sum
    filters: [churn_probability_band: ">60%"]
    sql: ${lost_profit_expected} ;;
    value_format_name: "usd"
  }
}

