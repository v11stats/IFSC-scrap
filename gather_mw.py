# I use multithreading to speed up scraping and store results in a queue for thread safety.


import requests
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import logging
from queue import Queue
# Athlete ID range (I checked manually)
# 1. Load the CSV with excluded IDs
try:
    excluded_ids_df = pd.read_csv('all_missing_ids.csv')
    # Convert the 'x' column to a set for fast lookups
    excluded_ids = set(excluded_ids_df['x'])
    print(f"Loaded {len(excluded_ids)} IDs to exclude.")
except FileNotFoundError:
    print("Warning: 'all_missing_ids.csv' not found. No IDs will be excluded.")
    excluded_ids = set()

# 2. Define the full range of IDs
start_id = 1
end_id = 17377
all_ids = range(start_id, end_id + 1)

# 3. Create the final list of IDs to scrape
ids_to_scrape = [athlete_id for athlete_id in all_ids if athlete_id not in excluded_ids]
print(f"Total IDs to scrape after exclusion: {len(ids_to_scrape)}")



# ## Athlete Information (birthday, country, height, etc)

# In[2]:


logging.basicConfig(filename='athlete_data_errors.log', level=logging.ERROR, format='%(asctime)s %(levelname)s: %(message)s')

def fetch_athlete_data(athlete_id, data_queue, failed_queue):
    headers = {
        'X-Csrf-Token': 'QsiFWuxEY1S9h_-dQgRA_7S5w9uvvmXsjq56QbTPw4i_g_XR68rMCBFFhW6HngBRtHskfN5yjX8GQmawqs8BlQ',
        'Referer': 'https://ifsc.results.info',
        'Cookie': 'session_id=_verticallife_resultservice_session=6RHN3xZrXnftTiScNfSHg7BVvuebLzGAmC9P5vIpzdySn2vG7VwQpjSZRDHug%2BPKCWlkt831HjLvHsPoVKrzTGsPVR6mqSOtjHB%2Bwht%2Bj39KxYO%2FJlaU6zmh8VhNFEl9bXHiOlPGk8AxnZqiBSYKTxJFCqh34nqdurXfFDcsRnbEtYCixcOdx%2F32E4zYGLVw7DSXXIKOVTUivS43UJZq5zDWPctX95UWm%2FD7%2B6UYT2s0B%2B3XJVPgjMWCMR%2FVZs%2FQC45Gjm4uCpHHe8Yt73nM3J%2Br43V1HuHGSvRpRczrJ4QdovlJHDEpg4rjUA%3D%3D',
    }
    url = f"https://ifsc.results.info/api/v1/athletes/{athlete_id}"
    
    try:
        response = requests.get(url, headers=headers, timeout=120)
        if response.status_code == 200:
            athlete_data = response.json()
            # Ensure we handle missing fields safely using .get()
            data_queue.put({
                'athlete_id': athlete_data.get('id', None),
                'firstname': athlete_data.get('firstname', None),
                'lastname': athlete_data.get('lastname', None),
                'age': athlete_data.get('age', None),
                'gender': athlete_data.get('gender', None),
                'country': athlete_data.get('country', None),
                'height': athlete_data.get('height', None),
                'arm_span': athlete_data.get('arm_span', None),
                'paraclimbing_sport_class': athlete_data.get('paraclimbing_sport_class', None),
                'birthday': athlete_data.get('birthday', None),
            })
        else:
            logging.error(f"Failed to fetch athlete ID {athlete_id}: Status {response.status_code}, Reason: {response.reason}")
            failed_queue.put(athlete_id)
    
    except Exception as e:
        logging.error(f"Error fetching data for athlete ID {athlete_id}: {e}")
        failed_queue.put(athlete_id)


# In[3]:


def retry_failed_athlete_info(failed_ids, max_retries=2, delay=2):
    retry_results = []
    failed_queue = Queue()

    for retry_count in range(max_retries):
        print(f"Retry attempt {retry_count + 1} for {len(failed_ids)} failed athlete IDs")
        retry_futures = []
        data_queue = Queue()
        
        with ThreadPoolExecutor(max_workers=20) as executor:
            retry_futures = {executor.submit(fetch_athlete_data, athlete_id, data_queue, failed_queue): athlete_id for athlete_id in failed_ids}
            failed_ids = []  # Reset failed_ids list for next retry
        
            for future in as_completed(retry_futures):
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Error during retry for athlete: {e}")
        
        # Collect results from queues
        while not data_queue.empty():
            retry_results.append(data_queue.get())
        
        while not failed_queue.empty():
            failed_ids.append(failed_queue.get())
        
        if not failed_ids:
            break  # Exit loop if no more failed IDs
        
        time.sleep(delay)  # Wait between retries to avoid overloading the server
    
    if failed_ids:
        print(f"Final failed athlete IDs after {max_retries} retries: {failed_ids}")
    
    return retry_results, failed_ids


# In[4]:


def scrape_athletes_parallel(athlete_ids, max_workers=25): # Changed signature
    athletes_info = []
    failed_ids = []
    data_queue = Queue()
    failed_queue = Queue()
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit tasks for each athlete ID from the provided list
        # --- Changed Line ---
        futures = {executor.submit(fetch_athlete_data, athlete_id, data_queue, failed_queue): athlete_id for athlete_id in athlete_ids}
        
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"Error during scraping: {e}")
    
    # Collect results from queues
    while not data_queue.empty():
        athletes_info.append(data_queue.get())
    
    while not failed_queue.empty():
        failed_ids.append(failed_queue.get())
    
    # Retry for failed athlete IDs
    if failed_ids:
        retry_results, failed_ids = retry_failed_athlete_info(failed_ids)
        athletes_info.extend(retry_results) # Add successful retries

    return athletes_info, failed_ids


# Scrape athlete data and create a DataFrame
# --- Changed Line ---
athletes_info_list, failed_ids = scrape_athletes_parallel(ids_to_scrape)
athletes_info_df = pd.DataFrame(athletes_info_list)

# Save the results to a CSV file
athletes_info_df.to_csv('athlete_information.csv', index=False)

print(f"Scraped {len(athletes_info_df)} athletes")
if failed_ids:
    print(f"Failed to fetch data for {len(failed_ids)} athlete IDs after retries: {failed_ids}")


# ## Athlete Results



logging.basicConfig(filename='scraping_errors.log', level=logging.ERROR, format='%(asctime)s %(levelname)s: %(message)s')

def fetch_athlete_results(athlete_id, results_queue, failed_queue):
    headers = {
        'X-Csrf-Token': 'QsiFWuxEY1S9h_-dQgRA_7S5w9uvvmXsjq56QbTPw4i_g_XR68rMCBFFhW6HngBRtHskfN5yjX8GQmawqs8BlQ',
        'Referer': 'https://ifsc.results.info',
        'Cookie': 'session_id=_verticallife_resultservice_session=6RHN3xZrXnftTiScNfSHg7BVvuebLzGAmC9P5vIpzdySn2vG7VwQpjSZRDHug%2BPKCWlkt831HjLvHsPoVKrzTGsPVR6mqSOtjHB%2Bwht%2Bj39KxYO%2FJlaU6zmh8VhNFEl9bXHiOlPGk8AxnZqiBSYKTxJFCqh34nqdurXfFDcsRnbEtYCixcOdx%2F32E4zYGLVw7DSXXIKOVTUivS43UJZq5zDWPctX95UWm%2FD7%2B6UYT2s0B%2B3XJVPgjMWCMR%2FVZs%2FQC45Gjm4uCpHHe8Yt73nM3J%2Br43V1HuHGSvRpRczrJ4QdovlJHDEpg4rjUA%3D%3D',
    }
    url = f"https://ifsc.results.info/api/v1/athletes/{athlete_id}"
    
    try:
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            athlete_data = response.json()
            results = []
            
            for result in athlete_data.get('all_results', []):
                # Use `.get()` with default values to avoid KeyErrors if fields are missing
                results.append({
                    'athlete_id': athlete_id,
                    'rank': result.get('rank', None),  # Use None if 'rank' is missing
                    'discipline': result.get('discipline', None),
                    'season': result.get('season', None),
                    'date': result.get('date', None),
                    'event_id': result.get('event_id', None),
                    'event_location': result.get('event_location', None),
                    'd_cat': result.get('d_cat', None),
                })
            results_queue.put(results)
        else:
            logging.error(f"Failed to fetch athlete ID {athlete_id}: Status {response.status_code}, Reason: {response.reason}")
            failed_queue.put(athlete_id)
    
    except Exception as e:
        logging.error(f"Error fetching results for athlete ID {athlete_id}: {e}")
        failed_queue.put(athlete_id)



def retry_failed_athletes(failed_ids, max_retries=3, delay=2):
    retry_results = []
    failed_queue = Queue()

    for retry_count in range(max_retries):
        print(f"Retry attempt {retry_count + 1} for {len(failed_ids)} failed athlete IDs")
        retry_futures = []
        results_queue = Queue()
        
        with ThreadPoolExecutor(max_workers=20) as executor:
            retry_futures = {executor.submit(fetch_athlete_results, athlete_id, results_queue, failed_queue): athlete_id for athlete_id in failed_ids}
            failed_ids = []  # Reset failed_ids list for next retry
        
            for future in as_completed(retry_futures):
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Error during retry for athlete: {e}")
        
        # Collect results from queues
        while not results_queue.empty():
            retry_results.extend(results_queue.get())
        
        while not failed_queue.empty():
            failed_ids.append(failed_queue.get())
        
        if not failed_ids:
            break  # Exit loop if no more failed IDs
        
        time.sleep(delay)  # Wait between retries to avoid overloading the server
    
    if failed_ids:
        print(f"Final failed athlete IDs after {max_retries} retries: {failed_ids}")
    
    return retry_results, failed_ids



def scrape_athlete_results_parallel(athlete_ids, max_workers=60): # Changed signature
    athlete_results = []
    failed_ids = []
    results_queue = Queue()
    failed_queue = Queue()
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # --- Changed Line ---
        futures = {executor.submit(fetch_athlete_results, athlete_id, results_queue, failed_queue): athlete_id for athlete_id in athlete_ids}
        
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"Error during scraping: {e}")
    
    # Collect results from queues
    while not results_queue.empty():
        athlete_results.extend(results_queue.get())
    
    while not failed_queue.empty():
        failed_ids.append(failed_queue.get())
    
    # Retry for failed athlete IDs
    if failed_ids:
        retry_results, failed_ids = retry_failed_athletes(failed_ids)
        athlete_results.extend(retry_results)  # Add successful retries
    
    return athlete_results, failed_ids


# In[9]:


# --- Changed Line ---
athlete_results_list, failed_ids = scrape_athlete_results_parallel(ids_to_scrape)
athlete_results_df = pd.DataFrame(athlete_results_list)

athlete_results_df.to_csv('athlete_results.csv', index=False)

print(f"Scraped results for {len(athlete_results_df)} athlete events")
if failed_ids:
    print(f"Failed to fetch data for {len(failed_ids)} athlete IDs after retries: {failed_ids}")
