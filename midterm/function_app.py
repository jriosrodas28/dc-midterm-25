import azure.functions as func
import spacy
import logging
import json
import time
from azure.storage.blob import BlobServiceClient

# Load the spaCy language model
nlp = spacy.load('en_core_web_sm')


nlp.max_length = 100_000_000  

STORAGE_ACCOUNT_URL = "https://ncf2025largediststore.blob.core.windows.net"
DEFAULT_FILE_NAME = "spy_file_10_MB.txt"
DEFAULT_START = 0
DEFAULT_END = 1_000_000  # 1 MB in bytes

app = func.FunctionApp()

# Function to handle blob download in chunks
def download_blob_in_chunks(blob_client, start, end):
    # Download the file from 'start' to 'end' directly
    chunk_data = blob_client.download_blob(offset=start, length=end - start).readall()
    return chunk_data


# Function to process text with SpaCy
def pos_tagging_spacy(text):
    doc = nlp(text)
    tagged_sentences = []
    sentence_count = 0
    for sentence in doc.sents:
        sentence_count += 1
        tagged_tokens = [(token.text, token.pos_) for token in sentence]
        tagged_sentences.append(tagged_tokens)
    return tagged_sentences, sentence_count

# Function to count POS tags
def count_pos_tags(tagged_sentences):
    pos_counts = []
    for sentence in tagged_sentences:
        pos_count = {'NOUN': 0, 'VERB': 0, 'ADJ': 0, 'ADV': 0, 'PRON': 0, 'DET': 0}
        for word, tag in sentence:
            if tag == 'NOUN':
                pos_count['NOUN'] += 1
            elif tag == 'VERB':
                pos_count['VERB'] += 1
            elif tag == 'ADJ':
                pos_count['ADJ'] += 1
            elif tag == 'ADV':
                pos_count['ADV'] += 1
            elif tag == 'PRON':
                pos_count['PRON'] += 1
            elif tag == 'DET':
                pos_count['DET'] += 1
        pos_counts.append(pos_count)
    return pos_counts

# Function to calculate sentence complexity
def calculate_complexity(tagged_sentences, pos_counts):
    complexities = []

    for sentence, pos_count in zip(tagged_sentences, pos_counts):
        # Count the total number of tokens in the sentence
        num_tokens = len(sentence)

        # Calculate complexity score using POS tags
        # Weights for each POS tag
        noun_weight = 2
        verb_weight = 1.5
        adj_weight = 1
        adv_weight = 1
        pron_weight = 1
        det_weight = 0.5

        # Sum the weighted POS counts
        weighted_pos_count = (pos_count['NOUN'] * noun_weight +
                              pos_count['VERB'] * verb_weight +
                              pos_count['ADJ'] * adj_weight +
                              pos_count['ADV'] * adv_weight +
                              pos_count['PRON'] * pron_weight +
                              pos_count['DET'] * det_weight)

        # Calculate complexity score: higher weighted POS count and more tokens increase complexity
        complexity_score = weighted_pos_count / num_tokens

        # Append the complexity score for the sentence
        complexities.append(complexity_score)

    return complexities


@app.route(route="nlp-process", auth_level=func.AuthLevel.ANONYMOUS)
def nlp_process(req: func.HttpRequest) -> func.HttpResponse:
    # Track the start time of the entire function
    function_start_time = time.time()

    try:
        file_name = req.params.get('file_name', DEFAULT_FILE_NAME)
        start = int(req.params.get('start', DEFAULT_START))
        end = int(req.params.get('end', DEFAULT_END))

        # Connect to Azure Blob Storage
        blob_service_client = BlobServiceClient(account_url=STORAGE_ACCOUNT_URL)
        container_client = blob_service_client.get_container_client("spy-files")
        blob_client = container_client.get_blob_client(file_name)

        # Fetch blob properties to get total file size
        blob_properties = blob_client.get_blob_properties()
        total_file_size = blob_properties.size

        # Safety checks for start and end boundaries
        start = max(0, start)
        end = min(end, total_file_size)

        if start >= end:
            return func.HttpResponse(
                json.dumps({"error": "'start' must be less than 'end' after adjustment."}),
                status_code=400,
                mimetype="application/json"
            )

        # Download the specified chunk
        download_start_time = time.time()
        chunk_data = download_blob_in_chunks(blob_client, start, end)
        download_duration = time.time() - download_start_time
        logging.info(f"Downloaded chunk of size {len(chunk_data)} bytes in {download_duration:.4f}s.")

        # Process the chunk data
        text = chunk_data.decode('utf-8')
        tagged_sentences, sentence_count = pos_tagging_spacy(text)
        
        # Process results and generate response
        pos_counts = count_pos_tags(tagged_sentences)
        complexities = calculate_complexity(tagged_sentences, pos_counts)
        #average_complexity = sum(complexities) / len(complexities) if complexities else 0
        #avg_pos_counts = {tag: sum([count[tag] for count in pos_counts]) / len(pos_counts) for tag in pos_counts[0].keys()} if pos_counts else {}
        
        # Calculate total function duration
        function_duration = time.time() - function_start_time

        result = {
            'file_name': file_name,
            'sentence_count': sentence_count,
            'pos_tag_counts': pos_counts,
            'complexities': complexities,
            #'average_sentence_complexity': average_complexity,
            #'avg_pos_counts': avg_pos_counts,
            'file_metadata': {
                'total_file_size_bytes': total_file_size,
                'total_chunks_possible': total_file_size 
            },
            
            'timing': {
                'download_duration_seconds': round(download_duration, 4),
                'function_duration_seconds': round(function_duration, 4)
            }
        }

        return func.HttpResponse(
            json.dumps(result),
            mimetype="application/json"
        )

    except Exception as e:
        logging.error(f"Error processing the file: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500
        )
