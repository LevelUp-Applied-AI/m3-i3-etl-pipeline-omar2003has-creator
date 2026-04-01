"""Tests for the ETL pipeline.

Write at least 3 tests:
1. test_transform_filters_cancelled — cancelled orders excluded after transform
2. test_transform_filters_suspicious_quantity — quantities > 100 excluded
3. test_validate_catches_nulls — validate() raises ValueError on null customer_id
"""
import pandas as pd
import pytest

from etl_pipeline import transform, validate 

def create_mock_data(status='completed', quantity=10):
    data = {
        'customers': pd.DataFrame({'customer_id': [1], 'customer_name': ['Omar']}),
        'products': pd.DataFrame({'product_id': [101], 'category': ['Tech'], 'unit_price': [50.0]}),
        'orders': pd.DataFrame({'order_id': [1], 'customer_id': [1], 'status': [status]}),
        'order_items': pd.DataFrame({'order_id': [1], 'product_id': [101], 'quantity': [quantity]})
    }
    return data

def test_transform_filters_cancelled():
    mock_data = create_mock_data(status='cancelled')
    
    result = transform(mock_data)
    
    assert len(result) == 0

def test_transform_filters_suspicious_quantity():

    mock_data = create_mock_data(quantity=150)
    
    result = transform(mock_data)
    
    assert len(result) == 0

def test_validate_catches_nulls():
    bad_df = pd.DataFrame({
        'customer_id': [None],
        'customer_name': ['Omar'],
        'total_orders': [1],
        'total_revenue': [50.0]
    })
    
    with pytest.raises(ValueError):
        validate(bad_df)