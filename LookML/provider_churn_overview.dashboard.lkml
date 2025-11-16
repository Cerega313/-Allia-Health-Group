dashboard: provider_churn_overview {
  title: "Provider Churn – Overview"
  layout: tile

  # -------- Filters ----------
  filter: period_month {
    title: "Filter by month"
    type: field_filter
    explore: provider_churn
    field: fact_provider_subscription_history.period_month
  }

  filter: state {
    title: "Filter by state"
    type: field_filter
    explore: provider_churn
    field: fact_provider_subscription_history.state
  }

  # -------- KPI tiles (top row) ----------

  element: kpi_new_subs {
    title: "New"
    type: single_value
    query: {
      explore: provider_churn
      fields: [fact_provider_subscription_history.new_subscriptions_count]
    }
    listen: {
      period_month: fact_provider_subscription_history.period_month
      state: fact_provider_subscription_history.state
    }
  }

  element: kpi_renewals {
    title: "Renewals"
    type: single_value
    query: {
      explore: provider_churn
      fields: [fact_provider_subscription_history.renewals_count]
    }
    listen: {
      period_month: fact_provider_subscription_history.period_month
      state: fact_provider_subscription_history.state
    }
  }

  element: kpi_reactivated {
    title: "Reactivated"
    type: single_value
    query: {
      explore: provider_churn
      fields: [fact_provider_subscription_history.reactivated_subscriptions_count]
    }
    listen: {
      period_month: fact_provider_subscription_history.period_month
      state: fact_provider_subscription_history.state
    }
  }

  element: kpi_frozen {
    title: "Frozen"
    type: single_value
    query: {
      explore: provider_churn
      fields: [fact_provider_subscription_history.frozen_subscriptions_count]
    }
    listen: {
      period_month: fact_provider_subscription_history.period_month
      state: fact_provider_subscription_history.state
    }
  }

  # -------- KPI tiles (second row) ----------

  element: kpi_up_for_renewal {
    title: "Up for renewal"
    type: single_value
    query: {
      explore: provider_churn
      fields: [fact_provider_subscription_history.up_for_renewal_count]
    }
    listen: {
      period_month: fact_provider_subscription_history.period_month
      state: fact_provider_subscription_history.state
    }
  }

  element: kpi_expiring_this_month {
    title: "Expiring this month"
    type: single_value
    query: {
      explore: provider_churn
      fields: [fact_provider_subscription_history.expiring_this_month_count]
    }
    listen: {
      period_month: fact_provider_subscription_history.period_month
      state: fact_provider_subscription_history.state
    }
  }

  element: kpi_churn_rate {
    title: "Churn rate"
    type: single_value
    query: {
      explore: provider_churn
      fields: [fact_provider_subscription_history.overall_churn_rate]
    }
    listen: {
      period_month: fact_provider_subscription_history.period_month
      state: fact_provider_subscription_history.state
    }
  }

  element: kpi_lost_profit {
    title: "Lost profit"
    type: single_value
    query: {
      explore: provider_churn
      fields: [fact_provider_subscription_history.total_lost_profit]
    }
    listen: {
      period_month: fact_provider_subscription_history.period_month
      state: fact_provider_subscription_history.state
    }
  }

  # -------- Chart: Churn rate over time ----------

  element: chart_churn_rate_over_time {
    title: "Churn rate over time"
    type: looker_line
    query: {
      explore: provider_churn
      fields: [
        fact_provider_subscription_history.period_month_month,
        fact_provider_subscription_history.overall_churn_rate
      ]
      sorts: [fact_provider_subscription_history.period_month_month]
    }
    listen: {
      state: fact_provider_subscription_history.state
    }
  }

  # -------- Chart: Churn rate by cycle type ----------

  element: chart_churn_rate_by_cycle {
    title: "Churn rate by cycle length"
    type: looker_line
    query: {
      explore: provider_churn
      fields: [
        fact_provider_subscription_history.period_month_month,
        fact_provider_subscription_history.cycle_length_months,
        fact_provider_subscription_history.overall_churn_rate
      ]
      pivots: [fact_provider_subscription_history.cycle_length_months]
      sorts: [fact_provider_subscription_history.period_month_month]
    }
    listen: {
      state: fact_provider_subscription_history.state
    }
  }

  # -------- Chart: Lost profit over time ----------

  element: chart_lost_profit_over_time {
    title: "Lost profit over time"
    type: looker_line
    query: {
      explore: provider_churn
      fields: [
        fact_provider_subscription_history.period_month_month,
        fact_provider_subscription_history.total_lost_profit
      ]
      sorts: [fact_provider_subscription_history.period_month_month]
    }
    listen: {
      state: fact_provider_subscription_history.state
    }
  }

}
