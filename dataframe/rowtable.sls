;; rowtable is used to describe list of row-oriented lists

(library (dataframe rowtable)
  (export
   dataframe->rowtable
   rowtable->dataframe)

  (import (rnrs)
          (dataframe record-types)
          (only (dataframe helpers)
                add1
                iota
                make-list
                transpose))

  (define (dataframe->rowtable df)
    (let* ([slist (dataframe-slist df)]
           [names (map series-name slist)]
           [ls-vals (map series-lst slist)])
      (cons names (transpose ls-vals))))

  (define (rowtable->dataframe rt header who)       
    (let* ([names (if header
                      (convert-header (car rt) who)
		      (make-header (length (car rt))))]
           [ls-vals (if header
                        (transpose (cdr rt))
                        (transpose rt))])
      (make-dataframe (make-slist names ls-vals))))

  (define (make-header n)
    (map string->symbol
	 (map string-append
	      (make-list n "V")
	      (map number->string (iota n)))))

  (define (convert-header lst who)
    ;; lst should only be comprised of strings or symbols
    (let loop ([lst lst]
               [empty-n 0]
               [out '()])
      (cond [(null? lst)
             (reverse out)]
            [(symbol? (car lst))
             (loop (cdr lst) empty-n (cons (car lst) out))]
            [(and (string? (car lst))
                  (member (car lst) '("" " ")))
             (loop (cdr lst) (add1 empty-n)
                   (cons (string->symbol
                          (string-append "X" (number->string empty-n)))
                         out))]
            [(string? (car lst))
             (loop (cdr lst) empty-n
                   (cons (string->symbol (replace-special (car lst) #\-)) out))]
            [else
             (assertion-violation who "lst should only contain symbols or strings")])))
  
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

  )

