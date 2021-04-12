import pdftotext
import requests
import io
import bs4
import re
import pandas as pd
import numpy as np
import json

def get_resolution_urls(start_year=1946, end_year=2021):
    '''
    Crawls through the UN Digital Library and fetches the URLs for each resolution's
    profile between the given start and end years.
    Input:
        start_year (int): the year program will start scraping from
        end_year (int): the year program will scrape the last
    Output:
        List of strings: URLs of each resolution's webpage
    '''
    all_urls = []
    for year in range(start_year, end_year + 1):
        for i in [1, 201]:
            url = 'https://digitallibrary.un.org/search?ln=en&c=Voting+Data&rg=200\
            &jrec={}&fct__3={}&fct__2=General+Assembly&fct__2=General+Assembly&cc=Voting+Data'.format(i, year)
            req = requests.get(url)
            soup = bs4.BeautifulSoup(req.text, 'html.parser')
            as_ = soup.find_all('a', class_='moreinfo', text='Detailed record')
            all_urls += [a['href'] for a in as_]
    return list(set(all_urls))


def get_metadata(urls):
    '''
    Given the URLs of the UNGA resolutions, fetches the metadata associated with
    each resolution.
    Input:
        urls (list): list of UNGA resolution URLs
            (output of the get_resolultion_urls() function)
    Output:
        List of dicts: contains metadata from all UNGA resolutions provided
            as input

    '''
    urls = list(map(lambda x: 'https://digitallibrary.un.org' + x, urls))
    metadata = []
    for url in urls:
        req = requests.get(url)
        soup = bs4.BeautifulSoup(req.text, 'html.parser')
        dic = {'url': url}
        divs = soup.find_all('div', class_='metadata-row')
        for div in divs:
            k = div.find_all('span')[0].text.strip()
            v = div.find_all('span')[1].text.strip()
            dic[k] = v
            as_ = div.find_all('a')
            for a in as_:
                dic[k + '_url'] = a['href']
        if 'Vote' in dic:
            decisions = ['Yes', 'No', 'Abstentions', 'Non-voting', 'Total']
            votes = re.findall(r':(\s+\S+)', dic['Vote summary'])
            for i, vote in enumerate(votes):
                votes[i] = vote.strip()
                if votes[i] == '|':
                    votes[i] = 0
                votes[i] = int(votes[i])
            dic['Votes'] = dict(zip(decisions, votes))
            dic['Votes_url'] = url.replace('?ln=en', '/export/xm')
        metadata.append(dic)
    return metadata


def get_voting_data(metadata):
    '''
    Given the metadata of UNGA resolutions, fetches the voting records for each resolution.
    Input:
        metadata (list of dicts): contains the metadata of all UNGA resolutions
            (output of the get_metadata() function)
    Output:
        List of dicts: matching each resolution ID with a dictionary of voting records.
    '''
    voting_data = {}
    for res in metadata:
        try:
            req = requests.get(res['Votes_url'])
        except KeyError:
            continue
        voting_data[res['Resolution']] = []
        soup = bs4.BeautifulSoup(req.text, 'html.parser')
        datafields = soup.find_all('datafield', tag='967')
        for field in datafields:
            votes = {}
            votes['Code'] = field.find_all('subfield', code='c')[0].text
            votes['Country'] = field.find_all('subfield', code='e')[0].text
            try:
                votes['Vote'] = field.find_all('subfield', code='d')[0].text
                voting_data[res['Resolution']].append(votes)
            except IndexError:
                voting_data[res['Resolution']].append(votes)
                continue
    return voting_data


def get_pdf_urls(metadata):
    pdf_urls = {}
    for res in metadata:
        try:
            req = requests.get(res['Resolution_url'].replace('?ln=en', '/export/xm'))
        except KeyError:

            continue
        soup = bs4.BeautifulSoup(req.text, 'html.parser')
        subfields = soup.find_all('subfield', code='u')
        for sf in subfields:
            if sf.text.endswith('-EN.pdf'):
                pdf_urls[res['Resolution']] = sf.text
                break
    return pdf_urls


#fetch and write URLs to a txt file
urls = get_resolution_urls(start_year=2021, end_year=2021)
with open('urls.txt', 'w') as outfile:
    json.dump(urls, outfile)

#fetch and write metadata to a txt file
metadata = get_metadata(urls)
with open('metadata.txt', 'w') as outfile:
    json.dump(metadata, outfile)

#fetch and write voting data to a txt file
voting_data = get_voting_data(metadata)
with open('voting_data.txt', 'w') as outfile:
    json.dump(voting_data, outfile)

#fetch and write PDF URLs to a txt file
pdf_urls = get_pdf_urls(metadata)
with open('pdf_urls.txt', 'w') as outfile:
    json.dump(pdf_urls, outfile)



