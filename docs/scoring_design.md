# Component Priority Scoring Design

## Overview

This document describes the design decisions and methodology behind the component priority scoring system.

## Scoring Philosophy

The scoring system is designed to balance multiple competing factors:

1. **Demand**: Historical demand indicates component popularity
2. **Availability**: In-stock items with short lead times are preferred
3. **Economics**: Lower prices and MOQs make components more accessible
4. **Reliability**: Authorized sources and documented parts reduce risk

## Feature Engineering Decisions

### Logarithmic Transformations

Many features in the dataset have extreme skewness:
- Inventory ranges from 0 to 387,855
- Prices range from cents to thousands of dollars
- MOQs range from 1 to 10,000+

We use `log1p(x) = log(1 + x)` to:
- Compress the range while preserving order
- Handle zero values gracefully
- Make the distribution more normal

### Inverse Transformations

For "lower is better" metrics, we use `1 / (1 + x)`:
- Maps high values → low scores
- Maps low values → high scores
- Bounded between 0 and 1
- Smooth, continuous transformation

### Composite Features

**Availability Score** combines multiple signals:
```
availability_score = 
    in_stock * 0.5 +           # Being in stock is most important
    immediate_availability * 0.3 +  # Zero lead time is valuable
    (inventory / moq) * 0.2    # Ratio indicates how many orders can be fulfilled
```

## Scoring Methodology

### Three-Tier Approach

1. **Pre-filtering**: Remove items that are completely unavailable
   - No inventory AND lead time > 12 weeks
   
2. **Base Scoring**: Weighted sum of normalized features
   - Each feature contributes based on business importance
   
3. **Boost Factors**: Multiplicative rewards for exceptional items
   - Ample stock (10x MOQ)
   - Immediate availability
   - Authorized sources

### Weight Selection

Initial weights were set based on business priorities:
- **Demand & Availability** (25% each): Primary drivers
- **Lead Time** (15%): Critical for planning
- **Price & MOQ** (10% each): Economic factors
- **Authorization & Documentation** (10% & 5%): Risk mitigation

## Normalization Strategy

We use RobustScaler for feature normalization because:
- Less sensitive to outliers than MinMaxScaler
- Uses median and IQR instead of mean and std
- Better for skewed distributions

Final scores use MinMaxScaler to ensure 0-100 range.

## Future Improvements

### Phase 2: Machine Learning

1. **Feature Importance**
   - Random Forest to identify most predictive features
   - SHAP values for interpretability
   
2. **Weight Optimization**
   - Grid search over weight combinations
   - Optimize for conversion rate or revenue
   
3. **Dynamic Scoring**
   - Time-based adjustments (seasonal demand)
   - Category-specific weights
   - Supplier performance integration

### Data Quality Enhancements

1. **Demand Metrics**
   - Trend analysis (growing vs declining)
   - Seasonality detection
   - Forecast integration
   
2. **Supply Chain Signals**
   - Supplier reliability scores
   - Geographic diversity
   - Alternative part suggestions

### A/B Testing Framework

1. **Variation Testing**
   - Multiple scoring algorithms
   - Different weight configurations
   - Business rule variations
   
2. **Success Metrics**
   - Click-through rate
   - Conversion rate
   - Revenue per search
   - User satisfaction

## Performance Considerations

### Scalability

- Current: 10K rows in ~1 second
- Target: 1M rows in < 1 minute
- BigQuery: Billion rows with distributed processing

### Optimization Opportunities

1. **Columnar Processing**
   - Use Parquet for better compression
   - Leverage BigQuery native functions
   
2. **Caching**
   - Pre-compute expensive features
   - Cache scores for popular items
   
3. **Incremental Updates**
   - Only rescore changed items
   - Daily batch updates vs real-time

## Monitoring and Validation

### Key Metrics

1. **Score Distribution**
   - Avoid clustering at extremes
   - Maintain good separation
   
2. **Business Impact**
   - Conversion rate by score decile
   - Revenue attribution
   
3. **Data Quality**
   - Missing value rates
   - Feature coverage
   - Score stability over time