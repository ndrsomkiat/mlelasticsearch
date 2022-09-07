classdef HttpRequest < handle
    properties (Access = private)
        % HTTP connection option
        httpOption
        credential
        % HTTP Header field
        jsonHeader
        ndjsonHeader
        
    end

    methods (Access = public)
        function this = HttpRequest(cert_file, username, password)
            % Proxy server is disable by default
            this.httpOption = matlab.net.http.HTTPOptions('UseProxy', 0, 'ConnectTimeout', 30, 'VerifyServerName', false);
            this.jsonHeader = matlab.net.http.HeaderField('Content-Type', 'application/json');
            this.ndjsonHeader =  matlab.net.http.HeaderField('Content-Type', 'application/x-ndjson');
            
            if ~isempty(cert_file)
                this.httpOption.CertificateFilename = cert_file;
            end

            if ~isempty(username)
                this.credential = matlab.net.http.Credentials('Scheme', 'basic', 'Username', username, 'Password', password);
            end

        end

        % ========= JSON Things =========
        function ret = getJSON(this, res)
            ret = this.extractJsonBodyMessage(res);
        end

        function response = createGetJsonRequest(this, varargin)
            n = nargin - 1;
            if n < 1
                error('URI is required parameter')
            end
            
            URI = varargin{1};
            
            if n == 2
                body = varargin{2};
                response = this.createRequest('GET', URI, this.jsonHeader, body);
            else
                response = this.createRequest('GET', URI, this.jsonHeader);
            end
        end
        
        function response = createPostJsonRequest(this, URI, body)
            response = this.createRequest('POST', URI, this.jsonHeader, body);
        end

        function response = createPutJsonRequest(this, URI, body)
            response = this.createRequest('PUT', URI, this.jsonHeader, body);
        end
        % ========= JSON Things (end) ========= 

        % ========= NDJSON Things =========
        function reponse = createPostNdJsonRequest(this, URI, json_text)
            reponse = this.createRequest('POST', URI, this.ndjsonHeader, json_text);
        end
        % ========= NDJSON Things (end) =========
    end

    methods (Access = private)
        % Create HTTP Request
        % �y�������z
        %       1st params : GET, POST, PUT, DELETE, ... 
        %                  : matlab.net.http.RequestMethod
        %       2nd params : URI
        %       3rd params : (optional) Http Header
        %       4th params : (optional) Http Body message
        function response = createRequest(this, varargin)
            r = matlab.net.http.RequestMessage;
            n = nargin - 1;
            if n < 2
                error('createRequest function require at least 2 parameters (methods and uri)')
            end

            r.Method = varargin{1};
            if ~isa(varargin{2}, 'char')
                error('createRequest support only URI character array type');
            end
            scope = matlab.net.URI(varargin{2});
            this.credential.Scope = scope;

            %% -- Debug --
            % disp(this.httpOption);
            % disp(scope);
            % disp(this.credential);
            this.httpOption.Credentials = this.credential;
            %% -- end Debug --            
            
            if n >= 3 
                r.Header = varargin{3};
            end

            if n >= 4
                r.Body = matlab.net.http.MessageBody;
                r.Body.Data = varargin{4};
                %? We hack iff only body data already converted according to content-type
                %? then we could possibly use the raw-data as payload (char vector)
                if isa(varargin{4}, 'char')
                    r.Body.Payload = varargin{4};
                end
                %% -- Debug --
                % if isa(varargin{4}, 'struct')
                %     disp(jsonencode(varargin{4}));
                % else
                %     disp(varargin{4})
                % end
                %% -- end Debug --
            end

            try
                [response, ~, ~] = r.send(scope, this.httpOption);
            catch ME
                if ~isempty(ME.message)
                    msg = ME.message;
                else
                    msg = "";
                end
                warning(['HTTP exception', msg]);
                %! Is gone going to conflict with other error code ?
                response.StatusCode = matlab.net.http.StatusCode.Gone;
            end
        end
    end
end