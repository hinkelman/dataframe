(library (dataframe record-types)
  (export
   dataframe?
   dataframe-slist
   dataframe-names
   dataframe-dim
   make-df*
   make-dataframe
   make-series*
   make-series
   make-slist
   series?
   series-name
   series-lst
   series-length)

  (import (rnrs)
          (dataframe types))

  ;; series -------------------------------------------------------------------

  (define-record-type series
    ;; series-src is not exported
    (fields name src lst type length)
    (protocol
     (lambda (new)
       (lambda (name src)
         (let* ([type (guess-type src 1000)]
                [lst (convert-type src type)])
           (new name src lst type (length lst)))))))

  (define-syntax make-series*
    (syntax-rules ()
      [(_ (name vals ...))
         (make-series (quote name) (list vals ...))]))
  
  ;; dataframe ----------------------------------------------------------------

  (define-record-type dataframe
    (fields slist names dim)
    (protocol
     (lambda (new)
       (lambda (slist)
         (let* ([names (map series-name slist)]
                [rows (series-length (car slist))]
                [cols (length names)])
           (new slist names (cons rows cols)))))))

  (define-syntax make-df*
    (syntax-rules ()
      [(_ (name vals ...) ...)
       (let ([names (list (quote name) ...)]
             [ls-vals (list (list vals ...) ...)])
         (make-dataframe (make-slist names ls-vals)))]))
  
  (define (make-slist names ls-vals)
    (map (lambda (name vals)
           (make-series name vals))
         names ls-vals))

    ;; lots of checking that will be performed every time a dataframe is created
  ;; this currently allows for creating a dataframe with no rows
  ;; even though none of the dataframe procedures will accept a df with zero rows
  ;; (define (check-alist alist who)
  ;;   (when (null? alist)
  ;;     (assertion-violation who "alist is empty"))
  ;;   (unless (list? alist)
  ;;     (assertion-violation who "alist is not a list"))
  ;;   (unless (list? (car alist))
  ;;     (assertion-violation who "(car alist) is not a list"))
  ;;   (let ([names (map car alist)])
  ;;     (check-names-symbol names who)
  ;;     (check-names-unique names who))
  ;;   (unless (for-all (lambda (col) (list? (cdr col))) alist)
  ;;     (assertion-violation who "values are not a list"))
  ;;   (let ([col-lengths (map length alist)])
  ;;     ;; if only one column don't need to check equal length
  ;;     (unless (or (= (length col-lengths) 1)
  ;;                 (apply = (map length alist)))
  ;;       (assertion-violation who "columns not all same length"))))
  
  )

