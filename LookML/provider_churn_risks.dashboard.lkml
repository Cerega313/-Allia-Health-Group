dashboard: provider_churn_risks {
  title: "Provider Churn – Risks"
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

  # -------- KPI tiles ----------

  element: kpi_grace_period {
    title: "In grace period"
    type: single_value
    query: {
      explore: provider_churn
      fields: [fact_provider_subscription_history.providers_in_grace_period_count]
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

  # -------- Chart: Churn probability bands ----------

  element: chart_churn_probability_bands {
    title: "Churn probability bands"
    type: looker_bar
    query: {
      explore: provider_churn
      fields: [
        fact_provider_subscription_history.churn_probability_band,
        fact_provider_subscription_history.providers_by_band
      ]
      sorts: [fact_provider_subscription_history.churn_probability_band]
    }
    listen: {
      period_month: fact_provider_subscription_history.period_month
      state: fact_provider_subscription_history.state
    }
  }

  # -------- Chart: Potential lost profit for high-risk by cycle ----------

  element: chart_high_risk_lost_profit_by_cycle {
    title: "Potential lost profit (high-risk providers)"
    type: looker_column
    query: {
      explore: provider_churn
      fields: [
        fact_provider_subscription_history.cycle_length_months,
        fact_provider_subscription_history.high_risk_lost_profit_by_cycle
      ]
      sorts: [fact_provider_subscription_history.cycle_length_months]
    }
    listen: {
      period_month: fact_provider_subscription_history.period_month
      state: fact_provider_subscription_history.state
    }
  }

  # -------- Table: churned and at-risk providers ----------

  element: table_churned_and_at_risk {
    title: "Churned and at-risk providers"
    type: table
    query: {
      explore: provider_churn
      fields: [
        fact_provider_subscription_history.provider_id,
        fact_provider_subscription_history.provider_name,
        fact_provider_subscription_history.state,
        fact_provider_subscription_history.program_name,
        fact_provider_subscription_history.cycle_length_months,
        fact_provider_subscription_history.subscription_status,
        fact_provider_subscription_history.churn_label,
        fact_provider_subscription_history.churn_probability,
        fact_provider_subscription_history.lost_profit_expected
      ]
      filters: {
        fact_provider_subscription_history.churn_label: "CHURNED,AT_RISK"
      }
      sorts: [fact_provider_subscription_history.churn_probability desc]
    }
    listen: {
      period_month: fact_provider_subscription_history.period_month
      state: fact_provider_subscription_history.state
    }
  }

}
