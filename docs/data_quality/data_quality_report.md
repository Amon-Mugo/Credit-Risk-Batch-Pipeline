# Credit Risk Batch Pipeline — Data Quality Report

Generated: 2026-07-19T16:34:57.524261+00:00

## Row Counts
- Raw dataset: 2,260,701
- Checkpoint (labeled/feature-engineered): 1,345,083
- Delta: -915,618 (-40.5%)
- npl_ratio_row_count: 84
- vintage_curve_row_count: 613

## Null Rates (checkpoint dataset)
| Column | Null % |
|---|---|
| is_default | 0.0% |
| grade | 0.0% |
| addr_state | 0.0% |
| purpose | 0.0% |
| vintage_year | 0.0% |
| loan_age_months | 0.17% |
| dti_bucket | 0.0% |
| has_prior_delinquency | 0.0% |

## Distribution Snapshot

### grade
| Value | % |
|---|---|
| B | 29.19% |
| C | 28.37% |
| A | 17.47% |
| D | 14.94% |
| E | 6.96% |
| F | 2.38% |
| G | 0.68% |

### dti_bucket
| Value | % |
|---|---|
| Low | 60.03% |
| Moderate | 39.44% |
| High | 0.5% |
| Unknown | 0.03% |

### purpose
| Value | % |
|---|---|
| debt_consolidation | 58.01% |
| credit_card | 21.95% |
| home_improvement | 6.5% |
| other | 5.79% |
| major_purchase | 2.19% |
| medical | 1.16% |
| small_business | 1.14% |
| car | 1.08% |
| moving | 0.7% |
| vacation | 0.67% |
| house | 0.54% |
| wedding | 0.17% |
| renewable_energy | 0.07% |
| educational | 0.02% |

### vintage_year
| Value | % |
|---|---|
| 2015 | 27.92% |
| 2016 | 21.79% |
| 2014 | 16.59% |
| 2017 | 12.59% |
| 2013 | 10.02% |
| 2018 | 4.19% |
| 2012 | 3.97% |
| 2011 | 1.61% |
| 2010 | 0.85% |
| 2009 | 0.34% |
| 2008 | 0.11% |
| 2007 | 0.02% |

### has_prior_delinquency
| Value | % |
|---|---|
| False | 80.73% |
| True | 19.27% |

## Drift vs Baseline

No baseline available — this run has been saved as the new baseline.