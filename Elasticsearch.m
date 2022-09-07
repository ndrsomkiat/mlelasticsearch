% Elasticsearch is a interface between MATLAB and Elasticsearch
%
% Example :
%
%   es_obj = Elasticsearch();
%   es_obj.createConnection('localhost', '9200');
%   es_obj.getIndices();
classdef Elasticsearch < handle
    properties (Access = private)
        host_ip              % Elasticsearch node IP
        host_port            % Elasticsearch node port
        URI                  % Formatted host_ip:host_port
        http_request         % HTTP request object
        default_query_size   % Elasticsearch default query size
        max_query_size       % Maximum query size per request
        max_total_query_size % Maximum query size
        connected_status     % Elasticsearch status
    end
    
    methods (Access = public)
        function this = Elasticsearch()
            this.http_request = [];
            this.default_query_size = 10;
            this.max_query_size = 5000;
            this.max_total_query_size = 5000000;
            this.connected_status = false;
        end
    end
    
    methods (Access = public)
        function ret = createConnection(this, ip, port, varargin)
            %createConnection - Create a connector to Elasticsearch
            %
            %   ret = es_obj.createConnection(this, ip, port)
            %
            % This function create a connector to Elasticsearch server by
            % specified IP address and Port. If the IP/Port is valid,
            %  return true/false as a result of server connection.
            %
            % [Args] : None
            % [Return]
            % ret : true  = response message equal to 200
            %       false = otherwise
            %
            % *The default connection timeout is set at 60 second.

            this.host_ip   = ip;
            this.host_port = port;
            this.URI = [ip, ':', port];
            this.connected_status = false;

            p = inputParser;
            addParameter(p, 'cert_file', "default", @(x) isa(x, 'char'));
            addParameter(p, 'username', "", @(x) isa(x, 'char'));
            addParameter(p, 'password', "", @(x) isa(x, 'char'));
            parse(p, varargin{:});

            % parse query size
            cert_file = p.Results.cert_file;
            this.http_request = HttpRequest(cert_file, p.Results.username, p.Results.password);
            
            ret = this.isElasticsearchAlive();
        end
        
        function ret = isElasticsearchAlive(this)
            %isElasticsearchAlive - get a status of Elasticsearch.
            %
            %   ret = es_obj.isElasticsearchAlive() 
            %
            % [Args] : None
            % [Return]
            % ret : true  = response message equal to 200
            %       false = otherwise
            
            if isempty(this.host_ip)
                this.connected_status = false;
            else
                uri = [this.URI, '/_cat/health'];
                try
                    res = this.http_request.createGetJsonRequest(uri);
                catch
                    warning('Internal error on connection');
                    res = [];
                end
                
                if res.StatusCode == matlab.net.http.StatusCode.OK
                    this.connected_status = true;
                else
                    this.connected_status = false;
                end
            end
            ret = this.connected_status;
        end
        
        function list = getAliases(this)
            % getAliases - get the list of aliases from Elasticsearch.
            %
            % list = es_obj.getAliases(); return a list of aliases.
            % 
            % [Args] : None
            % [Return]
            % list : (Nx1 cell char) list of cell that contains character vector.

            list = [];
            uri = [this.URI, '/_cat/aliases'];
            res = this.http_request.createGetJsonRequest(uri);
            if res.StatusCode == matlab.net.http.StatusCode.OK
                if ~isempty(res.Body.Data)
                    q = cellstr(splitlines(res.Body.Data));
                    q = q(~cellfun('isempty',q));
                    q = cellfun(@(s) strsplit(s, ' '), q, 'UniformOutput', false);
                    q = vertcat(q{:});
                    list = q(:, 1);
                end
            end
        end
        
        function list = getIndices(this)
            % getAliases - get the list of indices from Elasticsearch.
            %
            % [Args] : None
            % [Return]
            % list = es_obj.getAliases(); return a list of aliases.
            % list : (Nx1 cell char) list of cell that contains character vector.

            list = [];
            uri = [this.URI, '/_cat/indices'];
            res = this.http_request.createGetJsonRequest(uri);
            if res.StatusCode == matlab.net.http.StatusCode.OK
                if ~isempty(res.Body.Data)
                    q = cellstr(splitlines(res.Body.Data));
                    q = q(~cellfun('isempty',q));
                    q = cellfun(@(s) strsplit(s, ' '), q, 'UniformOutput', false);
                    q = vertcat(q{:});
                    list = q(:, 3);
                end
            end
        end
        
        function count = countDocs(this, index)
            % countDocs - get the number of documents existed in any index(indices).
            %
            % count = es_obj.countDocs('product_xxyy*') returns number of documents 
            % that related to 'product_xxyy*' indices from Elasticsearch.
            %
            % [Args]
            % index : (char) index name could be with wildcard
            % see more path parameters <index>
            %   https://www.elastic.co/guide/en/elasticsearch/reference/current/search-count.html
            % [Return]
            % count : (double) a number of documents

            count = this.countDocsByCondition(index, []);
        end
        
        function count = countDocsByCondition(this, index, condition)
            % countDocs - get the number of documents existed in any index(indices)
            % under the specified query.
            % 
            % query_field = {'Name', 'John'}
            % count = es_obj.countDocs('product_xxyy*', query_field) 
            % returns number of documents  that related to 'product_xxyy*' indices 
            % which have field 'Name' equal to 'John'.
            % [Args] 
            % index : (char) index name could be with wildcard
            % see more path parameters <index>
            %   https://www.elastic.co/guide/en/elasticsearch/reference/current/search-count.html
            % [Return]
            % count : (double) a number of documents

            count = 0;
            uri = [this.URI, '/', index, '/_count'];
            
            if ~isempty(condition)
                % Condition bool must match {field:value}
                s.query.bool.must.match = this.createFieldConditionStruct(condition);
                % Send the Elasticsearch format
                res = this.http_request.createPostJsonRequest(uri, s);
            else
                res = this.http_request.createGetJsonRequest(uri);
            end
            if res.StatusCode == matlab.net.http.StatusCode.OK
                count = res.Body.Data.count;
            end
        end
        
        function result = refreshIndices(this)
            % refreshIndices - refresh all indices
            %
            % status = es_obj.refreshIndices() returns true when success with response msg 200.
            % otherwise, return false.
            % [Args] : none
            % [Return]
            % ret : true  = response message equal to 200
            %       false = otherwise
            % see more path parameters <index>
            %   https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-refresh.html
            result = false;
            uri = [this.URI, '/_refresh'];
            res = this.http_request.createGetJsonRequest(uri);
            
            if res.StatusCode == matlab.net.http.StatusCode.OK
                result = true;
            end
        end
        
        function [data, data_size] = getData(this, p_index, varargin)
            %GETDATA query data from Elasticsearch server by specified index
            % [data, data_size] = getData(index, Name, Value) 
            % query Elasticsearch and return document value
            %
            % [Args] 
            % p_index : (char) index name could be with wildcard
            % Name, Value : (Name-value pair) 
            % 'field'   (1xN cell of char) must contain specified field
            %           equivalent to _source.includes = [values] with 
            %
            % 'must_exist' (logical) flag of query to force query only not-empty data
            %           [query.bool.must.exists.field = value]
            %
            % 'search'  (1xN cell of char) pair value between field and its value.
            %           equivalent to query.bool.must.term = {'field', 'value'}
            %           for more information about name-value pair see, 
            %           help Elasticsearch.createFieldConditionStruct
            %
            % 'size'    (double) maximum documents count to be queried
            %           the default value is 10
            %
            % 'sortname' (char) sorting field
            % 'sortby'   (char) 'asc' or 'desc' (required sortname field)
            %           equivalent to sort.(sortname).order = sortby
            %
            % 'custom_query' (char) create your own custom query
            %           equivalent to query.bool.must = jsonencode(custom_query)
            %           ex.1 : query all the data that have field name <date>
            %                 and greater than <datetime value>
            %
            %                  s.range.<date>.gt = <datetime value>
            %                  [tb_data, tb_size] = es_obj.getData('*', 'custom_query', s);
            %
            %           ex.2 : get a sorted list of dates from <index>
            %                  [tb_data, tb_size] = this.es_con.getData( index, ...
            %                                         'field', {'date'}, ...
            %                                         'sortname', 'date', ...
            %                                         'sortby', 'asc', ...
            %                                         'size', 100);
            
            % --- Phase variable
            p = inputParser;
            
            % Index as top search level
            addRequired(p, 'index');
            % Source
            addParameter(p, 'field' , [],@(x) any([isa(x, 'cell'), isa(x, 'char')]));
            addParameter(p, 'must_exist', false)
            % Sort
            validateSortby =  @(x) any(validatestring(x, {'asc', 'desc'}));
            addParameter(p, 'sortname', [], @(x) isa(x, 'char'));
            addParameter(p, 'sortby', 'desc',validateSortby);
            % Scroll & size
            addParameter(p, 'from', 0);
            addParameter(p, 'size'  , this.default_query_size);
            % Query
            addParameter(p, 'search', [],@(x) isa(x, 'cell'));
            addParameter(p, 'id', []);
            addParameter(p, 'range', []);
            
            % Custom Query
            addParameter(p, 'custom_query', []);
            
            parse(p, p_index, varargin{:});
            
            index = p.Results.index;
            
            % --- Building query DSL
            % _source query
            field = p.Results.field;
            if isa(field, 'cell')
                field_size = length(field);
            elseif ~isempty(field)
                field_size = 1;
                field = {field};
            else
                field_size = 0;
            end
            
            % existing field only
            if p.Results.must_exist
                if field_size > 0
                    s.xx_source.includes = field;
                    s_cells = cell(field_size, 1);
                    for i = 1:field_size
                        s_cells{i}.exists.field = field{i};
                    end
                    s.query.bool.must = s_cells;
                end
            end
            
            % matching field with its value
            if ~isempty(p.Results.search)
                st_search = this.createFieldConditionStruct(p.Results.search);
                if field_size > 0
                    s.query.bool.must{end+1}.term = st_search;
                else
                    s.query.bool.must.term = st_search;
                end
            end
            
            if ~isempty(p.Results.id)
                s.query.bool.filter.term.xx_id = p.Results.id;
            end
            
            % Sort & size
            if p.Results.size > this.max_query_size
                disp(['Query size : ', num2str(p.Results.size)])
                warning('Query a large data from elasticsearch might cause problem!')
                s.size = this.max_query_size;
            else
                s.size = p.Results.size;
            end
            
            if ~isempty(p.Results.sortname)
                s.sort.(p.Results.sortname).order = p.Results.sortby;
            end
            s.from = p.Results.from;
            
            % Custom query
            q = p.Results.custom_query;
            if ~isempty(q)
                q_len = length(q);
                if q_len == 1
                    s.query.bool.must = q(1);
                end
                for i=2:q_len
                    s.query.bool.must{end+1} = q(i);
                end
            end
            
            % --- Send the Elasticsearch format
            data = [];
            data_size = 0;
            
            % Enable search_after only user using sort
            if ~isempty(p.Results.sortname) && p.Results.from == 0
                % Do not allow anything query more than this maximum size
                if p.Results.size > this.max_total_query_size
                    warning(['Too much size, the query size is set to ', num2str(this.max_total_query_size)]);
                    upper_bound = this.max_total_query_size;
                else
                    upper_bound = p.Results.size;
                end
            else
                upper_bound = 1;
                if p.Results.size > this.max_query_size
                    warning(['without sorting, the query size is set to ', num2str(s.size)]);
                end
            end
            
            % --- Query data
            for i = 1:this.max_query_size:upper_bound
                uri = [this.URI, '/', index, '/_search'];
                % Convert query struct to json format
                payload = this.createJsonencode(s);
                res = this.http_request.createPostJsonRequest(uri, payload);
                if res.StatusCode == matlab.net.http.StatusCode.OK
                    [st_data, st_data_size] = this.decodeElasticsearchData(res);
                    if ~isempty(st_data)
                        data_size = data_size + st_data_size;
                        % Query next search_after
                        if isfield(st_data, 'sort')
                            s.search_after = st_data.sort(end); % 2âÒà»ç~
                        end
                        % Concatenate the query result
                        data = this.concatStruct(data, st_data);
                    end
                elseif isfield(res, 'Body') || isprop(res, 'Body')
                    disp(res.Body.Data.error);
                    break;
                else
                    error('Unexpected internal error during getData');
                end
            end
            
            % --- Attempt to convert to table
            try
                if ~isempty(data)
                    processed_struct = cellfun(@(x) orderfields(x), data.data);
                    data.data = struct2table(processed_struct, 'AsArray', true);
                end
            catch
                %! Sometime elasticsearch return an empty field
                %! So, if something happen here all the program will failed
                error('Convert to table problem.');
            end
        end
        
        % Update a single field using ID
        % ret : true  = updated successfully
        %       false = cannot update
        % *this function refresh all indices.
        function ret = updateFieldByIdIdx(this, index, id, fieldname, value)
            s.(fieldname) = value;
            ret = this.update(index, id, s);
        end
        
        % Update a single ID using struct
        % ret : true  = updated successfully
        %       false = cannot update
        function ret = update(this, index, id , s)
            ret = true;
            s_doc.doc = s;
            % Send the Elasticsearch format
            uri = [this.URI, '/', index, '/_update/', id];
            res = this.http_request.createPostJsonRequest(uri, s_doc);
            if res.StatusCode ~= matlab.net.http.StatusCode.OK
                warning(['Update failed: ', uri]);
                ret = false;
            end
            
            % Refresh indices
            if ~this.refreshIndices()
                warning('Cannot refresh after bulk');
            end
        end
        
        % Create a new mapping to Elasticsearch by custom structure
        % [Args]
        % index : (char) index name 
        % s : (struct) Elasticsearch schema mapping in json format (converted from struct)
        % For json format see,
        % https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping.html
        % [Return]
        % ret : true  = updated successfully
        %       false = cannot update        
        function ret = createMapping(this, index, s)
            ret = false;
            if ~isa(index, 'char')
                error('id must be character vector');
            end
            uri = [this.URI, '/', index];
            
            % Send
            s = this.createJsonencode(s);
            res = this.http_request.createPutJsonRequest(uri, s);
            if res.StatusCode == matlab.net.http.StatusCode.OK
                ret = true;
            else
                warning('Mapping failed')
            end
        end
        
        % Create a doc to Elasticsearch by custom structure
        % [Args]
        % index : (char) index name 
        % id : (char) 'auto' or custom id
        % s : (struct) Elasticsearch schema mapping in json format (converted from struct)
        % For json format see,
        % https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping.html
        % [Return]
        % ret : true  = updated successfully
        %       false = cannot update        
        %
        % *this function refresh all indices.
        
        function ret = create(this, index, id, s)
            ret = false;
            if ~isa(id, 'char')
                error('id must be character vector');
            end
            
            if strcmp(id, 'auto')
                % Building the JSON format first
                uri = [this.URI, '/', index, '/_doc'];
            else
                uri = [this.URI, '/', index, '/_doc/', id];
            end
            
            % Send
            s = this.createJsonencode(s);
            res = this.http_request.createPostJsonRequest(uri, s);
            if res.StatusCode == matlab.net.http.StatusCode.Created
                ret = true;
            else
                warning(['Create failed: ', uri]);
            end
            
            % Refresh indices
            if ~this.refreshIndices()
                warning('Cannot refresh after bulk');
            end
        end
        
        % Create a multiple ID using struct
        % ret : true  = created successfully
        %       false = cannot create
        function ret = createBulk(this, index, st_docs)
            body = '';
            docs_size = length(st_docs);
            
            % Create NdJson format
            for i=1:docs_size
                meta = struct();
                meta.index.xx_index = index;
                meta = this.createJsonencode(meta);
                if isa(st_docs(i), 'struct')
                    data = st_docs(i);
                elseif isa(st_docs(i), 'cell')
                    data = st_docs{i};
                else
                    error(['Bulk update not support type : ', class(st_docs(i))]);
                end
                data = this.createJsonencode(data);
                
                one_data = [meta, newline, data, newline];
                
                %! fix this memory leak
                body = [body, one_data];
            end
            ret = this.bulk(body);
        end
        
        % Update a multiple ID using struct
        % ret : true  = updated successfully
        %       false = cannot update
        function ret = updateBulk(this, index, id, st_docs)
            if ~isa(id, 'cell')
                error('invalid cell type');
            end
            
            body = '';
            id_size = length(id);
            % Create NdJson format
            for i=1:id_size
                meta = struct();
                meta.update.xx_index = index;
                meta.update.xx_id = id{i};
                meta.update.retry_on_conflict = 5;
                meta = this.createJsonencode(meta);
                data = struct();
                if isa(st_docs(i), 'struct')
                    data.doc = st_docs(i);
                elseif isa(st_docs(i), 'cell')
                    data.doc = st_docs{i};
                else
                    error(['Bulk update not support type : ', class(st_docs(i))]);
                end
                data = this.createJsonencode(data);
                
                one_data = [meta, newline, data, newline];
                
                %! fix this memory leak
                body = [body, one_data];
            end
            ret = this.bulk(body);
        end
        
        % Bulk API for update/create to document
        % body parameter will be in NdJson format only
        % for more information about Bulk API
        % https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
        % ret : true  = created or updated successfully
        %       false = cannot create or updated
        % *this function refresh all indices.
        function ret = bulk(this, body)
            ret = true;
            uri = [this.URI, '/_bulk'];
            % Push data to Elasticsearch server
            res = this.http_request.createPostNdJsonRequest(uri, body);
            if res.StatusCode ~= matlab.net.http.StatusCode.OK
                ret = false;
                if isfield(res, 'Body')
                    disp(res.Body.Data.error);
                end
                return
            end
            n = size(res.Body.Data.items(:), 1);
            
            % Check status foreach bulk result
            for i = 1:n
                items = res.Body.Data.items(i);
                field_status = fieldnames(items);
                % Bulk API will return only single field
                ret_code = items.(field_status{1}).status;
                if ret_code ~= matlab.net.http.StatusCode.OK && ...
                        ret_code ~= matlab.net.http.StatusCode.Created
                    ret = false;
                    try
                        disp(items.(field_status{1}).error);
                        disp(items.(field_status{1}).status);
                        disp(body{i*2})
                        disp(body{i*2+1})
                    catch
                        disp(items.(field_status{1}));
                    end
                end
            end
            
            % Refresh indices
            if ~this.refreshIndices()
                warning('Cannot refresh after bulk');
            end
        end
    end
    
    methods(Access = public, Static)
        function [st_ret, len] = decodeElasticsearchData(response)
            % Decode Elasticsearch HTTP response to data into beauty struct
            % [Args]
            % response : (struct) a result from http request class
            % [Return]
            % st_ret : (struct) a beauty struct (sorted by field name)
            % len : (double) number of queried documents
            st_ret = [];
            len = 0;
            
            if isempty(response.Body.Data) || isempty(response.Body.Data.hits.hits)
                return
            end
            data_struct = response.Body.Data.hits.hits;
            
            % Get the index value from the record
            data_field_name = fieldnames(data_struct);
            index_idx = find(strcmp(data_field_name, 'x_index'), 1, 'first');
            type_idx  = find(strcmp(data_field_name, 'x_type'), 1, 'first');
            id_idx    = find(strcmp(data_field_name, 'x_id'), 1, 'first');
            score_idx = find(strcmp(data_field_name, 'x_score'), 1, 'first');
            data_idx  = find(strcmp(data_field_name, 'x_source'), 1, 'first');
            sort_idx  = find(strcmp(data_field_name, 'sort'), 1, 'first');
            
            % Get the data (cell ?, character array ?)
            data_cell = struct2cell(data_struct);
            st_ret.index    = data_cell(index_idx, :);
            st_ret.datatype = data_cell(type_idx, :);
            st_ret.id       = data_cell(id_idx, :);
            st_ret.score    = data_cell(score_idx, :);
            st_ret.data     = data_cell(data_idx, :);
            if ~isempty(sort_idx)
                st_ret.sort     = data_cell(sort_idx, :);
            end
            
            % Return empty struct when there is no data in query result
            len = length(st_ret.data);
            if len <= 0
                st_ret = [];
                return
            end
            
            % Elasticsearch query must return struct, so that we can use the
            % table format otherwise this function is not support
            if ~isa(st_ret.data{1}, 'struct')
                error('Query result is not struct type.')
            end
            
            % Sorting the field of struct
            st_ret = orderfields(st_ret);
        end
        
        % Create Elasticsearch query struct by search_pair
        %
        %   search_pair: (1xN cell of char) of field and value at least 1 pair.
        %        { field_name_1, value_1, field_name_2, value_2, ...}
        %        ** size of cell's element must be even **
        %
        %        ex: search 'price' equal to 5
        %            and 'customer_name' that matches 'john'
        %            then the search_pair
        %            search_pair = { 'price' , 5, 'customer_name', 'john'}
        %
        %   query_struct: (struct) elasticsearch query
        %             query_struct.price = 5
        %             query_struct.customer_name = 'john'
        function query_struct = createFieldConditionStruct(search_pair)
            % pair value must be the factor of 2
            search_size = length(search_pair);
            if mod(search_size, 2) ~= 0 || isempty(search_pair)
                error('ERR: mismatch function usage');
            end
            
            s = [];
            pair_num = search_size / 2;
            
            % create query struct according to the name-value pairs num
            for i = 0:(pair_num - 1)
                field_index = int16(i*2 + 1);
                value_index = int16(i*2 + 2);
                field_name = search_pair{field_index};
                field_value = search_pair{value_index};
                c_split = regexp(field_name, '\.', 'split');
                s = setfield(struct(), c_split{:}, field_value);
            end
            
            query_struct = s;
        end
        
        % Create json string that may contain '_' underscore as first character
        % using prefix xx_ to define the field that starts with underscore
        function str_json = createJsonencode(st_param)
            prefix_underscore = '"xx_';
            s_json = jsonencode(st_param);
            if ~isempty(regexp(s_json, prefix_underscore, 'once'))
                str_json = strrep(s_json, prefix_underscore, '"_');
            else
                str_json = s_json;
            end
        end
        
        % Concatenate each field member of two struct
        function st_out = concatStruct(total_st_data, new_coming_st_data)
            %! This might cause heap overflow problem
            field_name = fieldnames(new_coming_st_data);
            field_size = size(field_name, 1);
            % First time concat
            if isempty(total_st_data)
                c_data = cell(field_size, 1);
            else
                c_data = struct2cell(total_st_data);
            end
            c_st_data = struct2cell(new_coming_st_data);
            for j = 1:field_size
                c_data{j,:} = [c_data{j,:} c_st_data{j,:}];
                st_out.(field_name{j}) = c_data{j,:};
            end
        end
        
        % Create UTC timestamp
        function timestamp = getUTCTimestamp()
            timestamp = datestr(datetime('now', 'TimeZone','UTC'), 'yyyy/mm/dd HH:MM:SS');
        end
        
        % Convert UTC to Asia/Tokyo
        function timestamp = convertUTC2AsiaTokyo(date_str)
            timestamp = datetime(date_str,  'InputFormat', 'yyyy/MM/dd HH:mm:ss', 'TimeZone', 'UTC');
            % Convert timezone then automatically remove the properties.
            timestamp.TimeZone = 'Asia/Tokyo';
            timestamp.TimeZone = '';
        end
        
        % Convert Asia/Tokyo to UTC
        function timestamp = convertAsiaTokyo2UTC(date_str)
            timestamp = datetime(date_str,  'InputFormat', 'yyyy/MM/dd HH:mm:ss', 'TimeZone', 'Asia/Tokyo');
            % Convert timezone then automatically remove the properties.
            timestamp.TimeZone = 'UTC';
            timestamp.TimeZone = '';
        end
    end
end