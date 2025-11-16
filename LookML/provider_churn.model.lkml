connection: "bigquery_allia"  

include: "/views/*.view.lkml"
include: "/dashboards/*.dashboard.lkml"

explore: provider_churn {
  from: fact_provider_subscription_history
  label: "Provider Churn"
  description: "Provider subscription, churn and risk metrics by month, cycle,state."
}
