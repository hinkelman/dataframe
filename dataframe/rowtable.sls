;; rowtable is a bad name to describe list of rows; as used in read-csv and write-csv in (chez-stats csv)

(library (dataframe rowtable)
  (export
   dataframe->rowtable
   rowtable->dataframe)

  (import (rnrs)
          (only (dataframe df)
                check-dataframe
                dataframe-names
                dataframe-values-map
                make-dataframe)
          (only (dataframe helpers)
                add-names-ls-vals
                check-integer-positive
                check-list
                iota
                make-list
                transpose))

  (define (dataframe->rowtable df)
    (check-dataframe df "(dataframe->rowtable df)")
    (let* ([names (dataframe-names df)]
           [ls-vals (dataframe-values-map df names)])
      (cons names (transpose ls-vals))))

  (define rowtable->dataframe
    (case-lambda
      [(rt)
       (rt->df rt #t #t 100)]
      [(rt header)
       (rt->df rt header #t 100)]
      [(rt header try->number)
       (rt->df rt header try->number 100)]
      [(rt header try->number try-max)
       (rt->df rt header try->number try-max)]))

  (define (rt->df rt header try->number try-max)
    (let ([who "(rowtable->dataframe rt)"])
      (check-list rt "rt" who)
      (check-integer-positive try-max "try-max" who)
      (unless (and (boolean? header) (boolean? try->number))
        (assertion-violation who "header and try->number must be boolean (#t or #f)"))
      (unless (apply = (map length rt))
        (assertion-violation who "all rows in rt must have same length")))            
    (let* ([names (if header
                      (convert-header (car rt))
		      (make-header (length (car rt))))]
           [ls-vals1 (if header
                         (transpose (cdr rt))
                         (transpose rt))]
           [ls-vals2 (if try->number
                         (convert-strings ls-vals1 try-max)
                         ls-vals1)])
      (make-dataframe (add-names-ls-vals names ls-vals2))))

  (define (convert-strings ls-vals try-max)
    (let* ([rows (length (car ls-vals))]
           [n (if (< rows try-max) rows try-max)]
           [indices (iota n)])
      (map (lambda (vals)
             (let ([vals-n (map (lambda (x) (list-ref vals x)) indices)])
               (if (for-all (lambda (x) (string->number-mod x)) vals-n)
                   (map string->number-mod vals)
                   vals)))
           ls-vals)))

  (define (string->number-mod x)
    (if (string? x) (string->number x) x))
  
  (define (make-header n)
    (map string->symbol
	 (map string-append
	      (make-list n "V")
	      (map number->string (iota n)))))

  (define (convert-header lst)
    (unless (for-all (lambda (x) (or (string? x) (symbol? x))) lst)
      (assertion-violation
       "(rowtable->dataframe rt)"
       "header row must be comprised of strings or symbols"))
    (map (lambda (x) (if (symbol? x)
                         x
                         (string->symbol (replace-special x #\-))))
         lst))

  ;; it's possible to convert strings with special characters into symbols
  ;; but they are difficult to work with
  (define (replace-special str replacement)
    (let ([lst (string->list str)])
      (list->string
       (map (lambda (x) (if (member x special) replacement x)) lst))))

  (define special
    '(#\` #\@ #\# #\( #\) #\+
      #\[ #\] #\{ #\} #\|
      #\; #\' #\" #\,
      #\space #\tab #\newline))
  
  ;; ;; after going through the exercise below of identifying the good symbols
  ;; ;; I realized that the list of problem characters would be shorter
  ;; (define upper-alpha
  ;;   (map (lambda (x) (integer->char (+ x 65))) (iota 26)))

  ;; (define lower-alpha
  ;;   (map char-downcase upper-alpha))

  ;; (define numbers
  ;;   (map (lambda (x) (integer->char (+ x 48))) (iota 10)))

  ;; ;; special characters that are accepted b/c convert to symbols
  ;; (define special
  ;;   '(#\~ #\! #\$ #\% #\^ #\& #\* #\- #\_ #\=
  ;;     #\: #\? #\/ #\. #\< #\>))

  ;; (define good-char
  ;;   (append upper-alpha lower-alpha numbers special))

  )

