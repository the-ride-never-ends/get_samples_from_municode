import numpy as np

def analyze_token_distributions(documents):
    # Across-document average
    doc_lengths = [len(doc) for doc in documents]
    across_avg = np.mean(doc_lengths)
    
    # Within-document average
    total_tokens = sum(doc_lengths)
    within_avg = total_tokens / len(documents)
    
    # Additional context
    length_variance = np.var(doc_lengths)
    length_ratio = max(doc_lengths) / min(doc_lengths)
    
    return {
        "across_document_avg": across_avg,
        "within_document_avg": within_avg,
        "length_variance": length_variance,
        "max_min_ratio": length_ratio
    }

