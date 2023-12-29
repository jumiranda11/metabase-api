class Metabase_API():

  def __init__(self, domain, email, password=None, basic_auth=False):

    self.domain = domain.rstrip('/')
    self.email = email
    self.password = getpass.getpass(prompt='Please enter your password: ') if password is None else password
    self.session_id = None
    self.header = None
    self.auth = (self.email, self.password) if basic_auth else None
    self.authenticate()


  def authenticate(self):
    """Get a Session ID"""
    conn_header = {'username':self.email,
                   'password':self.password}

    res = requests.post(self.domain + '/api/session', json=conn_header, auth=self.auth)
    if not res.ok:
      raise Exception(res)

    self.session_id = res.json()['id']
    self.header = {'X-Metabase-Session':self.session_id}


  def validate_session(self):
    """Get a new session ID if the previous one has expired"""
    res = requests.get(self.domain + '/api/user/current', headers=self.header, auth=self.auth)

    if res.ok:  # 200
      return True
    elif res.status_code == 401:  # unauthorized
      return self.authenticate()
    else:
      raise Exception(res)



  ##################################################################
  ######################### REST Methods ###########################
  ##################################################################

  def get(self, endpoint, *args, **kwargs):
    self.validate_session()
    res = requests.get(self.domain + endpoint, headers=self.header, **kwargs, auth=self.auth)
    if 'raw' in args:
      return res
    else:
      return res.json() if res.ok else False

  def post(self, endpoint, *args, **kwargs):
    self.validate_session()
    res = requests.post(self.domain + endpoint, headers=self.header, **kwargs, auth=self.auth)
    if 'raw' in args:
      return res
    else:
      return res.json() if res.ok else False


  ###############################################################
  ##################### Helper Functions ########################
  ###############################################################

  def get_item_id(self, item_type, item_name, collection_id=None, collection_name=None, db_id=None, db_name=None, table_id=None):

    assert item_type in ['database', 'table', 'card', 'collection', 'dashboard', 'pulse', 'segment']

    if item_type in ['card', 'dashboard', 'pulse']:
      if not collection_id:
        if not collection_name:
          # Collection name/id is not provided. Searching in all collections
          item_IDs = [ i['id'] for i in self.get("/api/{}/".format(item_type)) if i['name'] == item_name
                                                                              and i['archived'] == False ]
        else:
          collection_id = self.get_item_id('collection', collection_name) if collection_name != 'root' else None
          item_IDs = [ i['id'] for i in self.get("/api/{}/".format(item_type)) if i['name'] == item_name
                                                                              and i['collection_id'] == collection_id
                                                                              and i['archived'] == False ]
      else:
        collection_name = self.get_item_name('collection', collection_id)
        item_IDs = [ i['id'] for i in self.get("/api/{}/".format(item_type)) if i['name'] == item_name
                                                                            and i['collection_id'] == collection_id
                                                                            and i['archived'] == False ]

      if len(item_IDs) > 1:
        if not collection_name:
          raise ValueError('There is more than one {} with the name "{}".\n\
              Provide collection id/name to limit the search space'.format(item_type, item_name))
        raise ValueError('There is more than one {} with the name "{}" in the collection "{}"'
                        .format(item_type, item_name, collection_name))
      if len(item_IDs) == 0:
        if not collection_name:
            raise ValueError('There is no {} with the name "{}"'.format(item_type, item_name))
        raise ValueError('There is no item with the name "{}" in the collection "{}"'
                        .format(item_name, collection_name))

      return item_IDs[0]


    if item_type == 'collection':
      collection_IDs = [ i['id'] for i in self.get("/api/collection/") if i['name'] == item_name ]

      if len(collection_IDs) > 1:
        raise ValueError('There is more than one collection with the name "{}"'.format(item_name))
      if len(collection_IDs) == 0:
        raise ValueError('There is no collection with the name "{}"'.format(item_name))

      return collection_IDs[0]


    if item_type == 'database':
      res = self.get("/api/database/")
      if type(res) == dict:  # in Metabase version *.40.0 the format of the returned result for this endpoint changed
        res = res['data']
      db_IDs = [ i['id'] for i in res if i['name'] == item_name ]

      if len(db_IDs) > 1:
        raise ValueError('There is more than one DB with the name "{}"'.format(item_name))
      if len(db_IDs) == 0:
        raise ValueError('There is no DB with the name "{}"'.format(item_name))

      return db_IDs[0]


    if item_type == 'table':
      tables = self.get("/api/table/")

      if db_id:
        table_IDs = [ i['id'] for i in tables if i['name'] == item_name and i['db']['id'] == db_id ]
      elif db_name:
        table_IDs = [ i['id'] for i in tables if i['name'] == item_name and i['db']['name'] == db_name ]
      else:
        table_IDs = [ i['id'] for i in tables if i['name'] == item_name ]

      if len(table_IDs) > 1:
        raise ValueError('There is more than one table with the name {}. Provide db id/name.'.format(item_name))
      if len(table_IDs) == 0:
        raise ValueError('There is no table with the name "{}" (in the provided db, if any)'.format(item_name))

      return table_IDs[0]


    if item_type == 'segment':
      segment_IDs = [ i['id'] for i in self.get("/api/segment/") if i['name'] == item_name
                                                                and (not table_id or i['table_id'] == table_id) ]
      if len(segment_IDs) > 1:
        raise ValueError('There is more than one segment with the name "{}"'.format(item_name))
      if len(segment_IDs) == 0:
        raise ValueError('There is no segment with the name "{}"'.format(item_name))

      return segment_IDs[0]


  ##################################################################
  ###################### Custom Functions ##########################
  ##################################################################

  def get_card_data(self, card_name=None, card_id=None, collection_name=None, collection_id=None, data_format='json', parameters=None):
    '''
    Run the query associated with a card and get the results.
    The data_format keyword specifies the format of the returned data:
      - 'json': every row is a dictionary of <column-header, cell> key-value pairs
      - 'csv': the entire result is returned as a string, where rows are separated by newlines and cells with commas.
    To pass the filter values use 'parameters' param:
      The format is like [{"type":"category","value":["val1","val2"],"target":["dimension",["template-tag","filter_variable_name"]]}]
      See the network tab when exporting the results using the web interface to get the proper format pattern.
    '''
    assert data_format in [ 'json', 'csv' ]
    if parameters:
      assert type(parameters) == list

    if card_id is None:
      if card_name is None:
        raise ValueError('Either card_id or card_name must be provided.')
      card_id = self.get_item_id(item_name=card_name,
                                 collection_name=collection_name,
                                 collection_id=collection_id,
                                 item_type='card')

    # add the filter values (if any)
    import json
    params_json = {'parameters':json.dumps(parameters)}

    # get the results
    res = self.post("/api/card/{}/query/{}".format(card_id, data_format), 'raw', data=params_json)

    # return the results in the requested format
    if data_format == 'json':
      return json.loads(res.text)
    if data_format == 'csv':
      return res.text.replace('null', '')
