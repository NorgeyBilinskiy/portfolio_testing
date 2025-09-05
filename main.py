"""
Main script for portfolio analysis
Calculates portfolio weights and downloads quotations for portfolio tickers
"""
from datetime import datetime

from loguru import logger
import pandas as pd

from src import IndexWeightsCalculator, QuotationsProcessor


def main():
    """
    Main function to calculate portfolio weights and download quotations
    """
    logger.info("Starting portfolio analysis...")
    
    try:
        logger.info("=" * 60)
        logger.info("STEP 1: CALCULATING PORTFOLIO WEIGHTS")
        logger.info("=" * 60)
        
        weights_calculator = IndexWeightsCalculator()
        portfolio_weights = weights_calculator.calculate_all_portfolios_weights()
        
        # Display weights summary
        print_weights_summary(portfolio_weights)
        
        # Step 2: Download quotations for portfolio tickers
        logger.info("=" * 60)
        logger.info("STEP 2: DOWNLOADING QUOTATIONS")
        logger.info("=" * 60)
        
        quotations_processor = QuotationsProcessor()
        all_quotations = quotations_processor.get_all_portfolios_quotations()
        
        # Display quotations summary
        print_quotations_summary(all_quotations)
        
        # Step 3: Create portfolio analysis report
        logger.info("=" * 60)
        logger.info("STEP 3: CREATING ANALYSIS REPORT")
        logger.info("=" * 60)
        
        create_analysis_report(portfolio_weights, all_quotations)
        
        logger.info("Portfolio analysis completed successfully!")
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        raise


def print_weights_summary(portfolio_weights):
    """
    Print formatted summary of portfolio weights
    
    Args:
        portfolio_weights (Dict[str, Dict[str, float]]): Portfolio weights dictionary
    """
    print("\n" + "="*80)
    print("PORTFOLIO WEIGHTS SUMMARY")
    print("="*80)
    
    for portfolio_name, weights in portfolio_weights.items():
        print(f"\n{portfolio_name.upper()}:")
        print("-" * 40)
        
        if not weights:
            print("  No data available")
            continue
            
        # Sort by weight (descending)
        sorted_weights = sorted(weights.items(), key=lambda x: x[1], reverse=True)
        
        total_weight = sum(weights.values())
        print(f"  Total weight: {total_weight:.4f} ({total_weight*100:.2f}%)")
        print(f"  Number of tickers: {len(weights)}")
        print()
        
        for ticker, weight in sorted_weights:
            print(f"  {ticker:8} | {weight:.4f} ({weight*100:6.2f}%)")
    
    print("\n" + "="*80)


def print_quotations_summary(all_quotations):
    """
    Print formatted summary of quotations data
    
    Args:
        all_quotations (Dict[str, Dict[str, pd.DataFrame]]): Quotations data dictionary
    """
    print("\n" + "="*80)
    print("QUOTATIONS DATA SUMMARY")
    print("="*80)
    
    for portfolio_name, quotations in all_quotations.items():
        print(f"\n{portfolio_name.upper()}:")
        print("-" * 40)
        
        if not quotations:
            print("  No quotations data available")
            continue
            
        print(f"  Number of tickers with data: {len(quotations)}")
        
        for ticker, df in quotations.items():
            if not df.empty:
                start_date = df.index[0] if len(df.index) > 0 else "N/A"
                end_date = df.index[-1] if len(df.index) > 0 else "N/A"
                record_count = len(df)
                print(f"  {ticker:8} | {record_count:6} records | {start_date} to {end_date}")
            else:
                print(f"  {ticker:8} | No data available")
    
    print("\n" + "="*80)


def create_analysis_report(portfolio_weights, all_quotations):
    """
    Create comprehensive analysis report combining weights and quotations data
    
    Args:
        portfolio_weights (Dict[str, Dict[str, float]]): Portfolio weights dictionary
        all_quotations (Dict[str, Dict[str, pd.DataFrame]]): Quotations data dictionary
    """
    logger.info("Creating comprehensive analysis report...")
    
    report_data = []
    
    for portfolio_name in portfolio_weights.keys():
        weights = portfolio_weights.get(portfolio_name, {})
        quotations = all_quotations.get(portfolio_name, {})
        
        # Get all unique tickers from both weights and quotations
        all_tickers = set(weights.keys()) | set(quotations.keys())
        
        for ticker in all_tickers:
            weight = weights.get(ticker, 0.0)
            df = quotations.get(ticker, pd.DataFrame())
            
            record_count = len(df) if not df.empty else 0
            start_date = df.index[0] if not df.empty and len(df.index) > 0 else "N/A"
            end_date = df.index[-1] if not df.empty and len(df.index) > 0 else "N/A"
            
            report_data.append({
                'portfolio': portfolio_name,
                'ticker': ticker,
                'weight': weight,
                'weight_percent': weight * 100,
                'has_quotations': not df.empty,
                'record_count': record_count,
                'start_date': start_date,
                'end_date': end_date
            })
    
    # Create DataFrame and save to CSV
    report_df = pd.DataFrame(report_data)
    report_filename = f"portfolio_analysis_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    report_df.to_csv(report_filename, index=False)
    
    logger.info(f"Analysis report saved to: {report_filename}")
    
    # Print summary statistics
    print("\n" + "="*80)
    print("ANALYSIS REPORT SUMMARY")
    print("="*80)
    
    for portfolio_name in portfolio_weights.keys():
        portfolio_data = report_df[report_df['portfolio'] == portfolio_name]
        
        print(f"\n{portfolio_name.upper()}:")
        print("-" * 40)
        print(f"  Total tickers: {len(portfolio_data)}")
        print(f"  Tickers with weights: {len(portfolio_data[portfolio_data['weight'] > 0])}")
        print(f"  Tickers with quotations: {len(portfolio_data[portfolio_data['has_quotations']])}")
        print(f"  Tickers with both: {len(portfolio_data[(portfolio_data['weight'] > 0) & (portfolio_data['has_quotations'])])}")
        
        # Show top 5 tickers by weight
        top_tickers = portfolio_data[portfolio_data['weight'] > 0].nlargest(5, 'weight')
        if not top_tickers.empty:
            print(f"\n  Top 5 tickers by weight:")
            for _, row in top_tickers.iterrows():
                print(f"    {row['ticker']:8} | {row['weight_percent']:6.2f}% | {row['record_count']:6} records")
    
    print("\n" + "="*80)


if __name__ == "__main__":
    # Configure logging
    logger.add("portfolio_analysis.log", rotation="1 day", retention="7 days", level="INFO")
    
    # Run main analysis
    main()
