"""
Main script for portfolio analysis
Calculates portfolio weights and downloads quotations for portfolio tickers
"""
from loguru import logger

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

        # Step 2: Download quotations for portfolio tickers
        logger.info("=" * 60)
        logger.info("STEP 2: DOWNLOADING QUOTATIONS")
        logger.info("=" * 60)
        
        quotations_processor = QuotationsProcessor()
        all_quotations = quotations_processor.get_all_portfolios_quotations()

        # Step 3: Create portfolio analysis report
        logger.info("=" * 60)
        logger.info("STEP 3: CREATING ANALYSIS REPORT")
        logger.info("=" * 60)

        logger.info("Portfolio analysis completed successfully!")
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        raise

if __name__ == "__main__":
    # Configure logging
    logger.add("portfolio_analysis.log", rotation="1 day", retention="7 days", level="INFO")
    
    # Run main analysis
    main()
