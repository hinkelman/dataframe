;; copied from helpers.ss; hashtable is overkill for this use case
(define (remove-duplicates ls)
  (let loop ([ls ls]
             [ht (make-hashtable equal-hash equal?)]
             [results '()])
    (cond [(null? ls)
           (reverse results)]
          [(hashtable-ref ht (car ls) #f)
           (loop (cdr ls) ht results)]
          [else
           ;; value is arbitrarily set to 0; key is what matters
           (hashtable-set! ht (car ls) 0) 
           (loop (cdr ls) ht (cons (car ls) results))])))

;; parts must be strings
(define (make-rel-path . parts)
  (let ([ds (string (directory-separator))])
    (let loop ([parts parts]
               [out "."])
      (if (null? parts)
          out
          (loop (cdr parts) (string-append out ds (car parts)))))))
  
;; read source code
(define (read-sls path)
  (with-input-from-file path read))

;; get all procedure definitions; 
(define (get-define-bodies lst)
  (filter (lambda (x) (and (pair? x) (symbol=? (car x) 'define))) lst))

;; define-body is one element of the list from get-define-bodies
(define (get-proc-name define-body)
  (if (pair? (cadr define-body))
      (caadr define-body)
      ;; cadr version for definitions using lambda or case-lambda
      (cadr define-body)))

(define (get-edges define-body all-names-nums)
  (let* ([proc-name (get-proc-name define-body)]
         [proc-num (cdr (assoc proc-name all-names-nums))]
         [out (let loop ([body (caddr define-body)]
                         [results '()])
                (cond [(null? body)
                       results]
                      [(not (pair? body))
                       (let ([name-num (assoc body all-names-nums)])
                         (if name-num (cons (cdr name-num) results) results))]
                      [else
                       (loop (car body) (loop (cdr body) results))]))])
    (map (lambda (x) (cons proc-num x)) (remove-duplicates out))))

;; assumes that you are working in main dataframe directory
(import (dataframe))
(define fldr "dataframe")
(define files (directory-list fldr))
(define define-bodies
  (apply
   append
   (map (lambda (file)
          (-> (make-rel-path fldr file)
              (read-sls)
              (get-define-bodies)))
        files)))
(define proc-names (map get-proc-name define-bodies))
(define export-names (cdaddr (read-sls "dataframe.sls")))
(define all-names (remove-duplicates (append export-names proc-names)))
(define all-names-nums
  (map (lambda (name num) (cons name num)) all-names (enumerate all-names)))
(define edges
  (apply append (map (lambda (db) (get-edges db all-names-nums)) define-bodies)))


(define (write-pairs lst car-proc cdr-proc path)
  (let ([p (open-output-file path)])
    (let loop ([lst lst])
      (cond [(null? lst)
             (close-port p)]
            [else
             (put-string p (string-append (car-proc (caar lst))
                                          (string #\tab)
                                          (cdr-proc (cdar lst))))
             (newline p)
             (loop (cdr lst))]))))

(write-pairs
 edges
 (lambda (x) (number->string x))
 (lambda (x) (number->string x))
 (make-rel-path "network-graph" "Edges.tsv"))

(write-pairs
 all-names-nums
 (lambda (x) (symbol->string x))
 (lambda (x) (number->string x))
 (make-rel-path "network-graph" "Nodes.tsv"))

  
       
                
                
         
