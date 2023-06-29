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
                iota
                make-list
                transpose))

  (define (dataframe->rowtable df)
    (check-dataframe df "(dataframe->rowtable df)")
    (let* ([names (dataframe-names df)]
           [ls-vals (dataframe-values-map df names)])
      (cons names (transpose ls-vals))))

  (define (rowtable->dataframe rt header?)
    (unless (and (list? rt)
                 (apply = (map length rt)))
      (assertion-violation "(rowtable->dataframe rt header?)"
			   "rt is not a rowtable"))
    (let ([names (if header?
                     (convert-header (car rt))
		     (make-header (length (car rt))))]
          [ls-vals (if header?
                       (transpose (cdr rt))
                       (transpose rt))])
      (make-dataframe (map cons names ls-vals))))

  (define (make-header n)
    (map string->symbol
	 (map string-append
	      (make-list n "V")
	      (map number->string (iota n)))))

  (define (convert-header lst)
    (unless (for-all (lambda (x) (or (string? x) (symbol? x))) lst)
      (assertion-violation
       "(rowtable->dataframe rt header?)"
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

