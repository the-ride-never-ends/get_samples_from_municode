import statistics as st


import numpy as np


from logger.logger import Logger
logger = Logger(logger_name=__name__)


def estimate_corpus_tokens(sample_tokens, sample_size, total_population_size):
    # Calculate sample statistics
    mean_tokens = np.mean(sample_tokens)
    std_tokens = np.std(sample_tokens, ddof=1)  # Using n-1 for sample std
    
    # Standard error of the mean
    sem = std_tokens / np.sqrt(sample_size)
    
    # Total estimate
    total_estimate = mean_tokens * total_population_size
    
    # Calculate margin of error (95% confidence)
    t_value = st.ppf(0.975, df=sample_size-1)
    margin_of_error = t_value * sem * total_population_size
    
    # Confidence interval for total
    ci_lower = total_estimate - margin_of_error
    ci_upper = total_estimate + margin_of_error
    
    # Coefficient of variation (to assess reliability)
    cv = (std_tokens / mean_tokens) * 100
    
    logger.info(f"""
    Estimated Total Tokens: {total_estimate:,.0f}
    Confidence Interval: ({ci_lower:,.0f}, {ci_upper:,.0f})
    Coefficient of Variation: {cv:.2f}%
    Relative Margin of Error: {(margin_of_error / total_estimate) * 100:.2f}%
    """, f=True)

    return total_estimate