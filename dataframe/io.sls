(library (dataframe io)
  (export
   csv->dataframe
   tsv->dataframe
   dataframe->csv
   dataframe->tsv
   dataframe-read
   dataframe-write)

  (import (rnrs)
          (srfi :6 basic-string-ports)
          (dataframe record-types)
          (dataframe rowtable)
          (only (dataframe helpers)
                add1
                sub1))

  (define dataframe-write
    (case-lambda
      [(df path) (dataframe-write df path #t)]
      [(df path overwrite)
       (when (and (file-exists? path) (not overwrite))
         (assertion-violation path "file already exists"))
       (when (file-exists? path)
         (delete-file path))
       (with-output-to-file path
         (lambda () (write df)))]))

  (define (dataframe-read path)
    (with-input-from-file path read))

  ;; ->dataframe -------------------------------------------------------------------

  (define csv->dataframe
    (case-lambda
      [(path) (csv->dataframe path #t)]
      [(path header)
       (rowtable->dataframe (read-delim path) header
                            "(csv->dataframe path)")]))

  (define tsv->dataframe
    (case-lambda
      [(path) (tsv->dataframe path #t)]
      [(path header)
       (rowtable->dataframe (read-delim path #\tab) header
                            "(csv->dataframe path)")]))

  ;; ->dataframe -------------------------------------------------------------------

  (define dataframe->csv
    (case-lambda
      [(df path) (dataframe->csv df path #t)]
      [(df path overwrite)
       (write-delim (dataframe->rowtable df) path #\, overwrite)]))

  (define dataframe->tsv
    (case-lambda
      [(df path) (dataframe->tsv df path #t)]
      [(df path overwrite)
       (write-delim (dataframe->rowtable df) path #\tab overwrite)]))

  ;; read-delim ---------------------------------------------------------------------
  
  (define read-delim
    (case-lambda
      [(path) (read-delim-helper path #\, +inf.0)]
      [(path sep-char) (read-delim-helper path sep-char +inf.0)]
      [(path sep-char max-rows) (read-delim-helper path sep-char max-rows)]))

  (define (read-delim-helper path sep-char max-rows)
    (let ([p (open-input-file path)])
      (let loop ([row (read-line p)]
		 [results '()]
		 [iter max-rows])
	(cond [(or (eof-object? row) (< iter 1))
	       (close-port p)
	       (reverse results)]
	      [else
	       (loop (read-line p)
		     (cons (parse-line row sep-char) results)
		     (sub1 iter))]))))
  
  ;; https://stackoverflow.com/questions/37858083/how-to-read-a-line-of-input-in-chez-scheme
  (define (read-line port)
    (define (eat p c)
      (if (and (not (eof-object? (peek-char p)))
	       (char=? (peek-char p) c))
	  (read-char p)))
    (let ([p (if (null? port) (current-input-port) port)])
      (let loop ([c (read-char p)]
		 [line '()])
	(cond [(eof-object? c) (if (null? line) c (list->string (reverse line)))]
	      [(char=? #\newline c) (eat p #\return) (list->string (reverse line))]
	      [(char=? #\return c) (eat p #\newline) (list->string (reverse line))]
	      [else (loop (read-char p) (cons c line))]))))

  ;; some csv files are quoted, i.e., "Date", and others are not, i.e., Date
  ;; parse-line always unquotes
  ;; parse-line also removes the double quotes, e.g., "Earvin ""Magic"" Johnson"
  (define (parse-line line sep-char)
    (let ([in (open-input-string line)])
      (let loop ([c (read-char in)]
                 [str ""]
                 [out '()]
                 [in-string #f])
        (cond [(eof-object? c)
               (reverse (cons str out))]
              ;; this case handles true separator (not in string)
              [(and (char=? c sep-char) (not in-string))
               (loop (read-char in) "" (cons str out) #f)]
              ;; this case drops the opening #\" of a quoted item
              [(and (char=? c #\")
                    (or (string=? str "")
                        (equal? (peek-char in) #\")))
               (loop (read-char in) str out #t)]
              ;; this case drops the closing #\" of a quoted item
              [(and (char=? c #\")
                    (or (equal? (peek-char in) sep-char)
                        (eof-object? (peek-char in))))
               (loop (read-char in) str out #f)]
              [else
               (loop (read-char in) (string-append str (string c)) out in-string)]))))
  
  ;; write-delim ----------------------------------------------------------

  (define write-delim
    (case-lambda
      [(lst path) (write-delim-helper lst path #\, #t)]
      [(lst path sep-char) (write-delim-helper lst path sep-char #t)]
      [(lst path sep-char overwrite) (write-delim-helper lst path sep-char overwrite)]))

  (define (write-delim-helper lst path sep-char overwrite)
    (when (and (file-exists? path) (not overwrite))
      (assertion-violation path "file already exists"))
    (when (and (file-exists? path) overwrite)
      (delete-file path))
    (let ([p (open-output-file path)])
      (let loop ([lst-local lst])
	(cond [(null? lst-local)
	       (close-port p)]
	      [else
	       (put-string p (delimit-list (car lst-local) sep-char))
	       (newline p)
	       (loop (cdr lst-local))]))))

  (define (delimit-list lst sep-char)
    (let loop ([lst lst]
	       [result ""]
	       [first? #t])
      (if (null? lst)
  	  result
  	  (let* ([item (car lst)]
                 ;; don't place sep-char before first item
  		 [sep-str (if first? "" (string sep-char))]
  		 [item-new (cond [(boolean? item) (if item "#t" "#f")]
                                 [(char? item) (string item)]
  				 [(symbol? item) (symbol->string item)]
  				 [(real? item) (number->string item)]
  				 [(string? item) (quote-string item sep-char)]
                                 [else "NA"])])
  	    (loop (cdr lst) (string-append result sep-str item-new) #f)))))

  (define (quote-string str sep-char)
    (let* ([in (open-input-string str)]
	   [str-list (string->list str)]
	   [str-length (length str-list)])
      (if (not (or (member sep-char str-list) (member #\" str-list)))
  	  str  ;; return string unchanged b/c no sep-char or double quotes
  	  (let loop ([c (read-char in)]
  		     [result "\""]
		     [ctr 0])
  	    (cond [(eof-object? c)
  		   (string-append result "\"")]
		  [(and (char=? c #\") (or (= ctr 0) (= ctr (sub1 str-length))))
		   ;; don't add double-quote character to string
		   ;; when it is at start or end of string
		   (loop (read-char in) (string-append result "") (add1 ctr))]
		  ;; 2x double-quotes for double-quotes inside string (not at start or end)
  		  [(char=? c #\")
  		   (loop (read-char in) (string-append result "\"\"") (add1 ctr))]
  		  [else
  		   (loop (read-char in) (string-append result (string c)) (add1 ctr))])))))
  
  )

