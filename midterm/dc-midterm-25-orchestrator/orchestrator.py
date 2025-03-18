import asyncio
import aiohttp
import logging
from datetime import datetime
from typing import List, Dict
import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Constants
CHUNK_SIZE_IN_MB = .1  # Adjust this value to your preferred chunk size (in MB)
CHUNK_SIZE = 1024 * 1024 * CHUNK_SIZE_IN_MB  # 1MB per chunk
CONCURRENT_REQUESTS = 8  # Number of parallel requests
CHUNK_SIZE = int(CHUNK_SIZE)

class DistributedOrchestrator:
    def __init__(self, endpoints: List[str]):
        self.endpoints = endpoints
        self.chunk_size = CHUNK_SIZE
        self.current_endpoint = 0
        self.processed_chunks = 0
        self.total_chunks = 0

    def _get_next_endpoint(self) -> str:
        """Rotate through the available function endpoints."""
        endpoint = self.endpoints[self.current_endpoint]
        self.current_endpoint = (self.current_endpoint + 1) % len(self.endpoints)
        return endpoint

    async def process_chunk(self, session: aiohttp.ClientSession, chunk: Dict, file_name: str, semaphore: asyncio.Semaphore) -> Dict:
        """Send a chunk to the NLP Azure Function for processing."""
        endpoint = self._get_next_endpoint()
        url = f"{endpoint}?file_name={file_name}&start={chunk['start']}&end={chunk['end']}"

        async with semaphore:
            try:
                async with session.get(url) as response:
                    if response.status == 200:
                        self.processed_chunks += 1
                        if self.processed_chunks % 10 == 0:
                            print(f"Progress: {(self.processed_chunks / self.total_chunks) * 100:.1f}%")
                        result = await response.json()
                        return result
                    else:
                        raise Exception(f"HTTP {response.status}: {await response.text()}")
            except Exception as e:
                logging.error(f"Error processing chunk {chunk['chunk_id']}: {str(e)}")
                raise
  
    async def get_file_metadata(self, session: aiohttp.ClientSession, file_name: str, semaphore: asyncio.Semaphore) -> int:
        """Fetch the file metadata from the first chunk."""
        print("Fetching file metadata...")
        first_chunk = {'chunk_id': 0, 'start': 0, 'end': 1}  # Smallest possible request
        response = await self.process_chunk(session, first_chunk, file_name, semaphore)

        if "file_metadata" in response and "total_file_size_bytes" in response["file_metadata"]:
            file_size = response["file_metadata"]["total_file_size_bytes"]
            print(f"Detected file size: {file_size} bytes")
            return file_size
        else:
            raise ValueError("File metadata not found in response.")

    async def process_file(self, file_name: str, batch_size: int = 10) -> List[Dict]:
        """Main method to process the file in chunks."""
        async with aiohttp.ClientSession() as session:
            file_size = await self.get_file_metadata(session, file_name)
             
            # Ensure file_size is an integer
            file_size = int(file_size)

            # Create chunks based on the file size
            chunks = [
                {'chunk_id': i, 'start': start, 'end': min(start + self.chunk_size, file_size)}
                for i, start in enumerate(range(0, file_size, self.chunk_size))
            ]

            self.total_chunks = len(chunks)
            self.processed_chunks = 0
            print(f"Chunk size: {self.chunk_size} bytes")
            print(f"Total number of chunks: {self.total_chunks}")
            start_time = datetime.now()

            tasks = []
            results = []

            semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)  # Limit concurrent requests

            for chunk in chunks:
                tasks.append(self.process_chunk(session, chunk, file_name, semaphore))
                if len(tasks) >= batch_size:
                    batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                    results.extend([r for r in batch_results if not isinstance(r, Exception)])
                    tasks = []

            if tasks:
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                results.extend([r for r in batch_results if not isinstance(r, Exception)])

            duration = datetime.now() - start_time
            print(f"\nProcessing completed in {duration.total_seconds():.1f} seconds")

            return results
        
    async def process_file(self, file_name: str, batch_size: int = 10) -> List[Dict]:
        """Main method to process the file in chunks."""
        async with aiohttp.ClientSession() as session:
            semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)  # Limit concurrent requests
            file_size = await self.get_file_metadata(session, file_name, semaphore)  # Pass semaphore here
         
            # Ensure file_size is an integer
            file_size = int(file_size)

            # Create chunks based on the file size
            chunks = [
                     {'chunk_id': i, 'start': start, 'end': min(start + self.chunk_size, file_size)}
                     for i, start in enumerate(range(0, file_size, self.chunk_size))
                    ]

            self.total_chunks = len(chunks)
            self.processed_chunks = 0
            print(f"Chunk size: {self.chunk_size} bytes")
            print(f"Total number of chunks: {self.total_chunks}")
            start_time = datetime.now()

            tasks = []
            results = []

            for chunk in chunks:
                tasks.append(self.process_chunk(session, chunk, file_name, semaphore))
                if len(tasks) >= batch_size:
                    batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                    results.extend([r for r in batch_results if not isinstance(r, Exception)])
                    tasks = []

            if tasks:
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                results.extend([r for r in batch_results if not isinstance(r, Exception)])

            duration = datetime.now() - start_time
            print(f"\nProcessing completed in {duration.total_seconds():.1f} seconds")

        return results



if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # List of function endpoints 
    endpoints = [
        "https://dcmidterm25-jrnlp.azurewebsites.net/api/nlp-process",
        "https://dcmidterm25-jrnlp.azurewebsites.net/api/nlp-process",
        "https://dcmidterm25-jrnlp.azurewebsites.net/api/nlp-process",   
        "https://dcmidterm25-jrnlp.azurewebsites.net/api/nlp-process"   
    ]

    file_name = "spy_file_1000_MB.txt"  # Blob file name

    orchestrator = DistributedOrchestrator(endpoints)
    results = asyncio.run(orchestrator.process_file(file_name, batch_size=10))


# Final result summary
print(f"Total chunks processed: {len(results)}")

# Initialize a list to hold the POS counts data
pos_data = []

# Initialize a list to hold the complexities
complexities = []

# Loop through each chunk in results and unpack the pos_tag_counts and complexities
for r in results:
    # Extract pos_tag_counts and complexities (assuming 'pos_tag_counts' and 'complexities' are available)
    pos_tag_counts = r.get('pos_tag_counts', [])
    chunk_complexities = r.get('complexities', [])  # List of complexities for this chunk

    # Ensure that the number of complexities matches the number of POS tag dictionaries (sentences)
    if len(pos_tag_counts) != len(chunk_complexities):
        raise ValueError("Number of complexities does not match the number of POS tag dictionaries in the chunk.")

    # Each dictionary in pos_tag_counts will be a new row, and complexities will be added as a column
    for pos_dict, complexity in zip(pos_tag_counts, chunk_complexities):
        # Add the POS dictionary as a row and include the corresponding complexity
        pos_dict['Complexity'] = int(complexity)  # Add complexity value to the dictionary
        pos_data.append(pos_dict)  # Append the updated dictionary to pos_data

# Create a DataFrame with POS counts, where each dictionary is a row and POS tags are columns
df_pos = pd.DataFrame(pos_data)

# Save DataFrame to CSV (you can specify the file path)
#df_pos.to_csv(r'C:\Users\james\OneDrive\Desktop\Code\DistComp\midterm\dc-midterm-25-orchestrator\pos_tag_frequencies.csv', index=False)
# Display the DataFrame
#print(df_pos)
mean_values = df_pos.mean()

# Create the summary report
summary_report = f"""
Summary Report: Average Sentence Tag and complexity
-------------------------
NOUN: {mean_values['NOUN']:.2f}
VERB: {mean_values['VERB']:.2f}
ADJ: {mean_values['ADJ']:.2f}
ADV: {mean_values['ADV']:.2f}
PRON: {mean_values['PRON']:.2f}
DET: {mean_values['DET']:.2f}
Complexity: {mean_values['Complexity']:.2f}
"""

# Print the summary report
print(summary_report)

# Create the histogram with more bins
fig, axes = plt.subplots(2, 2, figsize=(18, 12))

# Plot 1: Box plot for POS tag frequencies
axes[0, 0].boxplot(df_pos[['NOUN', 'VERB', 'ADJ', 'ADV', 'PRON', 'DET']].values, vert=False, patch_artist=True)
axes[0, 0].set_title('Box Plot of POS Tags')
axes[0, 0].set_yticklabels(['NOUN', 'VERB', 'ADJ', 'ADV', 'PRON', 'DET'])
axes[0, 0].set_xlabel('Frequency')
axes[0, 0].grid(True)

# Plot 2: Rolling Mean for POS Tags over Time
window_size = 100  # Set the window size for smoothing
df_rolling = df_pos[['NOUN', 'VERB', 'ADJ', 'ADV', 'PRON', 'DET']].rolling(window=window_size).mean()
df_rolling.plot(ax=axes[0, 1], legend=True)
axes[0, 1].set_title('Rolling Mean of POS Tag Frequencies Over Time')
axes[0, 1].set_xlabel('Sentence Index')
axes[0, 1].set_ylabel('Rolling Mean Frequency')
axes[0, 1].grid(True)

# Plot 3: Histogram for Sentence Complexity with more bins
axes[1, 0].hist(df_pos['Complexity'], bins=100, color='skyblue', edgecolor='black', density=True, alpha=0.7)
axes[1, 0].set_title('Distribution of Sentence Complexity')
axes[1, 0].set_xlabel('Sentence Complexity')
axes[1, 0].set_ylabel('Density')
axes[1, 0].grid(True)


# Plot 4: Optional additional plot (e.g., a second rolling mean or analysis)
axes[1, 1].axis('off')  # Leave this plot empty or set a new plot here

# Adjust layout to prevent overlap
plt.tight_layout()

# Show all the plots
#plt.show()

# Plotting the Kernel Density Estimate for the complexity values
sns.kdeplot(df_pos['Complexity'], color='red', shade=True, ax=axes[1, 0])

# Show the plot
plt.show()