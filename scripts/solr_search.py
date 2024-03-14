import math
import requests
import urllib

from tqdm.auto import trange


class SolrSearch:
    """
    Solr search class that takes a query and returns a DataFrame of search results
    """

    def __init__(self,
                 query,
                 base_url='https://metadata.idl.ucsf.edu/solr/ltdl3/select?q=',
                 rows=1000,
                 field_limiter='id,score,artifact',
                 format_='json',
                 only_opioids=True,
                 debug_solr=False,
                 ):

        self.query = query
        print(self.query)

        # set query format with parentheses
        if not only_opioids:  # if False don't limit to industry:Opioids
            self.query = '(' + query + ')'
        else:  # only_opioids = True only searches the industry:Opioids
            self.query = '(' + query + ' AND industry:Opioids)'

        # set base url we submit our query to
        self.base_url = base_url  # default: https://metadata.idl.ucsf.edu/solr/ltdl3/select?q=

        # set number of rows we want returned at a time
        self.rows = rows  # default: 1000, max = 1000

        # set field limiter
        self.field_limiter = field_limiter  # default = id, score

        # set format_
        self.format = format_  # default: 'json'

        # set query url replacing all spaces with %20
        self.url = (self.base_url + self.query + '&rows=' + str(
            self.rows) + '&fl=' + self.field_limiter + '&wt=' + self.format).replace(' ', '%20')
        print(self.url)
        if debug_solr:
            self.url = self.url + '&debug=results'

        # get response from server in json and decode it
        self.response = requests.get(self.url).json()

        # number results found for the submitted self.query
        self.number_found = self.response['response']['numFound']

        # create dictionary of ids and scores
        ids_and_scores = {x['id']: x['score'] for x in self.response['response']['docs']}
        # sort it by value in descending order
        self.ids_and_scores = dict(sorted(ids_and_scores.items(), key=lambda x: x[1], reverse=True))

        # dictionary of ids and artifacts
        try:
            self.ids_and_artifacts = {x['id']: x['artifact'] for x in self.response['response']['docs']}
        except KeyError:  # if there aren't any artifacts, like with movies hosted at archive.org we skip those
            self.ids_without_artifacts = []
            self.ids_and_artifacts = {}
            for x in self.response['response']['docs']:
                try:
                    self.ids_and_artifacts[x['id']] = x['artifact']
                except KeyError:
                    self.ids_without_artifacts.append(x['id'])

        # list of ids
        self.ids = sorted(self.ids_and_scores.keys())

        # number of results downloaded for the submitted self.query
        self.number_received = len(self.ids)

    def search(self, number=10000):
        """
        Get ids from searching self.query.

        If the request is for over 10,000 rows a "cursorMark" is necessary
        to access all of the results.
        """
        if str(number).lower() == 'all' or number > self.number_found:
            # set number to the total number of results found for our query
            number = self.number_found

        # get number of loops we'll run to request all the data and round up
        total_loops = math.ceil(number / self.rows)

        if number < self.number_found:
            print(f'Running query: {self.query}\nReturning {number:,d} of {self.number_found} results')
        else:
            print(f'Running query: {self.query}\nReturning {number:,d} results')

        # instantiate a temporary, empty dict to collect our ids and scores in
        temp_ids_and_scores_dict = {}

        if number > 10000:  # then we have to use cursorMarks

            # set cursorMark for first iteration as an asterisk
            self.cursorMark = '*'

            for i in trange(total_loops):
                # create url by adding adding sort by relevancy score + desc and
                # use id + asc sort for tie-breaker if matching relevancy scores
                url = self.url + '&sort=score+desc,id+asc' + '&cursorMark=' + self.cursorMark

                # get response from Solr API
                self.response = requests.get(url).json()

                # parse the new ids and scores from the response
                new_ids_and_scores_dict = {x['id']: x['score'] for x in self.response['response']['docs']}

                # add dict of new ids to collected ids
                temp_ids_and_scores_dict.update(new_ids_and_scores_dict)

                next_cursorMark = self.response['nextCursorMark']

                # quote_plus the next_cursorMark value so it is properly handled
                # in the case of special characters like '+'
                self.cursorMark = urllib.parse.quote_plus(next_cursorMark)

            # update our self.ids_dict and the total results in case we want to manually quit
            self.ids_dict = temp_ids_and_scores_dict
            print(f'Search results complete: {len(self.ids_dict):,d} results')


        else:  # we assume number <= 10000 and we request results with starting row

            # request data from the Solr API equal to
            # (total requested) / (number of rows requested at a time)
            for i in trange(total_loops):  # defaults: 10000 / 1000

                # set start value to iteration times number of rows requested as a string
                start = str(i * self.rows)

                # # create url by adding adding sort by relevancy score + desc and
                # use id + asc sort for tie-breaker if matching relevancy scores
                # followed by the starting result value
                url = self.url + '&sort=score+desc,id+asc' + '&start=' + start

                # get response from Solr API
                self.response = requests.get(url).json()

                # parse the new ids and scores from the response
                new_ids_and_scores_dict = {x['id']: x['score'] for x in self.response['response']['docs']}

                # add dict of new ids to collected ids
                temp_ids_and_scores_dict.update(new_ids_and_scores_dict)

        # update self.ids and self.number_received
        # NOTE: we don't update the search url or original response
        self.ids_and_scores = temp_ids_and_scores_dict
        self.ids = list(self.ids_and_scores.keys())
        self.scores = list(self.ids_and_scores.values())
        self.number_received = len(self.ids)
        print(f'Number of results received: {self.number_received:,d}\n')