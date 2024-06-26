select
    job_number,
    division,
    date_received,
    date_contacted,
    date_inspected,
    date_estimate_sent,
    date_started,
    date_closed,
    total_invoiced,
    total_job_cost,
    working_gp_pct,
    estimator,
    total_estimates,
    loss_category,
    date_estimate_approved,
    date_invoiced,
    date_of_cos,
    date_of_loss,
    date_of_majority_completion,
    date_of_work_authorization,
    foreman,
    insurance_carrier,
    job_name,
    loss_address,
    loss_city,
    loss_state,
    type_of_loss,
    loss_zip,
    marketing_person,
    referred_by_marketing_campaign,
    referred_by_contact,
    referred_by,
    referred_by_company,
    referred_by_contact_category,
    referred_by_contact_type,
    total_work_order_budget,
    subtrade_cost,
    materials_cost,
    other_cost,
    referral_fee_cost,
    total_collected,
    lien_rights_expiration,
    labor_cost,
    class,
    invoice_number,
    updated_date,
    job_status,
    secondary_loss_type,
    job_completion_percentage,
    referral_fee_date_paid,
    branch,
    # group,
    days_to_contact,
    days_to_inspect,
    days_to_send_estimate,
    days_to_authorize,
    days_to_start,
    days_to_majority_completion,
    days_to_paid_fee,
    days_to_invoice,
    days_to_close,
    consecutive_days,
    hours_to_contact,
    hours_to_inspect,
    hours_to_send_estimate,
    hours_to_authorize,
    hours_to_start,
    hours_to_majority_completion,
    hours_to_paid_fee,
    hours_to_invoice,
    hours_to_close,
    consecutive_hours,
    is_win,
    is_loss,
    is_pending,
    is_inprogress,
    is_incomplete,
    lead_to_contact,
    lead_to_inspect,
    bad_lead,
    week_date_received,
    week_date_started,
    month_date_received,
    month_date_started,
    year_date_received,
    year_date_started,
    win_total_invoiced_norm,
    total_invoiced_norm,
    data_issue,
    week_win_med_total_invoiced,
    month_win_med_total_invoiced,
    week_med_total_invoiced,
    month_med_total_invoiced,
    week_win_avg_total_invoiced,
    month_win_avg_total_invoiced,
    week_avg_total_invoiced,
    month_avg_total_invoiced,
    count(*)
from {{ ref("base_fedex_kpi") }}
group by
    job_number,
    division,
    date_received,
    date_contacted,
    date_inspected,
    date_estimate_sent,
    date_started,
    date_closed,
    total_invoiced,
    total_job_cost,
    working_gp_pct,
    estimator,
    total_estimates,
    loss_category,
    date_estimate_approved,
    date_invoiced,
    date_of_cos,
    date_of_loss,
    date_of_majority_completion,
    date_of_work_authorization,
    foreman,
    insurance_carrier,
    job_name,
    loss_address,
    loss_city,
    loss_state,
    type_of_loss,
    loss_zip,
    marketing_person,
    referred_by_marketing_campaign,
    referred_by_contact,
    referred_by,
    referred_by_company,
    referred_by_contact_category,
    referred_by_contact_type,
    total_work_order_budget,
    subtrade_cost,
    materials_cost,
    other_cost,
    referral_fee_cost,
    total_collected,
    lien_rights_expiration,
    labor_cost,
    class,
    invoice_number,
    updated_date,
    job_status,
    secondary_loss_type,
    job_completion_percentage,
    referral_fee_date_paid,
    branch,
    # group,
    days_to_contact,
    days_to_inspect,
    days_to_send_estimate,
    days_to_authorize,
    days_to_start,
    days_to_majority_completion,
    days_to_paid_fee,
    days_to_invoice,
    days_to_close,
    consecutive_days,
    hours_to_contact,
    hours_to_inspect,
    hours_to_send_estimate,
    hours_to_authorize,
    hours_to_start,
    hours_to_majority_completion,
    hours_to_paid_fee,
    hours_to_invoice,
    hours_to_close,
    consecutive_hours,
    is_win,
    is_loss,
    is_pending,
    is_inprogress,
    is_incomplete,
    lead_to_contact,
    lead_to_inspect,
    bad_lead,
    week_date_received,
    week_date_started,
    month_date_received,
    month_date_started,
    year_date_received,
    year_date_started,
    win_total_invoiced_norm,
    total_invoiced_norm,
    data_issue,
    week_win_med_total_invoiced,
    month_win_med_total_invoiced,
    week_med_total_invoiced,
    month_med_total_invoiced,
    week_win_avg_total_invoiced,
    month_win_avg_total_invoiced,
    week_avg_total_invoiced,
    month_avg_total_invoiced
having count(*) > 1
