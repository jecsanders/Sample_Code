import urllib.request
import xml.etree.ElementTree as ET
import datetime
import codecs
import os


# Make apisearch call and return results
def api_call(e_type, start_ps):
    publisherID = '2849821278263879'
    locations = 'houston'
    api_url = 'http://api.indeed.com/ads/apisearch?publisher=' + publisherID + '&format=xml&q=' + e_type + '&l=' + \
              locations + '%2C+tx&sort=&radius=&st=&jt=fulltime&start=' + str(start_ps) + '&limit=100&fromage=' \
              '&filter=&latlong=1&co=us&chnl=&userip=1.2.3.4&useragent=Mozilla/%2F4.0%28Firefox%29&v=2'
    data = urllib.request.urlopen(api_url)
    results = data.read()
    return results


# Define indeed data extraction function with parameter extract type
def extract_job_listings(e_type):

    # Declare datetime tim stamp, and local dir and file variables
    dt = datetime.datetime.today()
    c_dir = os.getcwd()
    csv_local_dir = c_dir + '\\test_job results\\'
    created_file = e_type.replace('+', '_') + '_' + str(dt).replace(':', '-').replace(' ', '_').split('.')[0] + '.csv'
    o_file = codecs.open(csv_local_dir + created_file, 'w', 'utf-8')
    o_file.write('city,company,jobtitle,jobkey,description,url,expired\n')

    # Call API function with e_type filter and return XML data
    results = api_call(e_type, 0)
    root = ET.fromstring(results)

    # Pagination code block
    total_results = 0
    for child in root:
        if 'totalresults' in child.tag:
            total_results = int(child.text)

    # Iterate through individual job postings
    i = 0
    for r in root.findall("./results/result"):
        i += 1
        r_city = str(r.find('city').text).replace(',', ' ')
        r_company = str(r.find('company').text).replace(',', ' ')
        r_jobtitle = str(r.find('jobtitle').text).replace(',', ' ')
        r_jobkey = str(r.find('jobkey').text).replace(',', ' ')
        r_snipp = str(r.find('snippet').text).replace(',', '|')
        r_expire = str(r.find('expired').text).replace(',', ' ')
        r_url = str(r.find('url').text).replace(',', ' ')
        if r_expire != 'True':
            data_row = r_city + ',' + r_company + ',' + r_jobtitle + ',' + r_jobkey + ',' + r_snipp + ',' + r_url + ','\
                       + r_expire + '\n'
            o_file.write(data_row)
        else:
            continue

    # If more than 100 results returned per query, call and paginate through rest of results
    if total_results > i:
        while i < total_results:
            results2 = api_call(e_type, i)
            root2 = ET.fromstring(results2)
            for r2 in root2.findall("./results/result"):
                i += 1
                r_city = str(r2.find('city').text).replace(',', ' ')
                r_company = str(r2.find('company').text).replace(',', ' ')
                r_jobtitle = str(r2.find('jobtitle').text).replace(',', ' ')
                r_jobkey = str(r2.find('jobkey').text).replace(',', ' ')
                r_snipp = str(r2.find('snippet').text).replace(',', '|')
                r_expire = str(r2.find('expired').text).replace(',', ' ')
                r_url = str(r2.find('url').text).replace(',', ' ')
                if r_expire != 'True':
                    data_row = r_city + ',' + r_company + ',' + r_jobtitle + ',' + r_jobkey + ',' + r_snipp + ',' + \
                               r_url + ',' + r_expire + '\n'
                    o_file.write(data_row)
                else:
                    continue
    else:
        pass
    print(created_file + ' has been created!!')
    o_file.close()

extract_job_listings('barista')
extract_job_listings('bank+teller')
extract_job_listings('server')
extract_job_listings('programmer')
extract_job_listings('bartender')
