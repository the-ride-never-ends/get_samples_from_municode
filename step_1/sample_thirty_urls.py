"""

250kb per page
Overhead 5kb per page
255kb * 3585 * 

id, top_nodes, random_node, subnodes, total_size
277304_San_Luis_Obispo, 38, 28, 0 subnodes, 338kb
306921_Volusia, 46, 3, 15 subnodes, 320kb
307626_Hendry,


"""



import csv
import random

def sample_urls_from_csv(csv_file_path, num_samples=30):
    """
    Example usage:
    sampled_urls = sample_urls_from_csv('your_csv_file.csv')
    print(sampled_urls)
    """
    urls = []
    with open(csv_file_path, 'r') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # Skip header row
        for row in reader:
            if row and len(row) > 0:
                url = row[0]  # Assuming URL is in the first column
                urls.append(url)
    
    if len(urls) < num_samples:
        return urls
    else:
        return random.sample(urls, num_samples)


