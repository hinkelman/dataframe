;; procedures ------------------------------------------------------

(define (remove-duplicates ls)
  (let loop ([ls ls]
             [results '()])
    (cond [(null? ls)
           (reverse results)]
          [(member (car ls) results)
           (loop (cdr ls) results)]
          [else
           (loop (cdr ls) (cons (car ls) results))])))

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

;; get all procedure definitions
(define (get-defs lst)
  (filter (lambda (x) (and (pair? x) (symbol=? (car x) 'define))) lst))

;; name is the procedure name
;; def is one element of the list from get-defs
(define (get-name def)
  (if (pair? (cadr def))
      (caadr def)
      ;; cadr version for definitions using lambda or case-lambda
      (cadr def)))

;; names-nums is list of pairs comprised of a procedure name and its arbitrary id #
;; returns list of pairs using the procedure id numbers
(define (get-edges def names-nums)
  (let* ([name (get-name def)]
         [num (cdr (assoc name names-nums))]
         [out (let loop ([body (caddr def)]
                         [results '()])
                (cond [(null? body)
                       results]
                      [(not (pair? body))
                       (let ([name-num (assoc body names-nums)])
                         (if name-num (cons (cdr name-num) results) results))]
                      [else
                       (loop (car body) (loop (cdr body) results))]))])
    (map (lambda (x) (cons num x)) (remove-duplicates out))))

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

;; data processing ---------------------------------------------------------
;; assumes that you are working in main dataframe directory
(import (dataframe))
(define fldr "dataframe")
(define files (directory-list fldr))

(define defs-by-file
  (map (lambda (file)
         (cons file 
               (-> (make-rel-path fldr file)
                   (read-sls)
                   (get-defs))))
       files))

(define names-by-file
  (apply append
         (map (lambda (dbf)
                (map (lambda (x)
                       (cons (car dbf) x))
                     (map get-name (cdr dbf))))
              defs-by-file)))

;; a few procedures are constructed and exported without being defined
(define exported-names-by-file
  (apply append
         (map (lambda (file)
                (map (lambda (x)
                       (cons file x))
                     (cdaddr (read-sls (make-rel-path fldr file)))))
              files)))

(define all-names-by-file
  (remove-duplicates (append exported-names-by-file names-by-file)))

(define all-names
  (map (lambda (name num) (cons name num))
       (map cdr all-names-by-file)
       (enumerate all-names-by-file)))

(define defs
  (apply append (map cdr defs-by-file)))
(define edges
  (apply append (map (lambda (def) (get-edges def all-names)) defs)))

(write-pairs
 edges
 (lambda (x) (number->string x))
 (lambda (x) (number->string x))
 (make-rel-path "network-graph" "Edges.tsv"))

(write-pairs
 all-names
 (lambda (x) (symbol->string x))
 (lambda (x) (number->string x))
 (make-rel-path "network-graph" "Nodes.tsv"))

(write-pairs
 all-names-by-file
 (lambda (x) x)
 (lambda (x) (symbol->string x))
 (make-rel-path "network-graph" "NodesByFile.tsv"))

  
       
                
                
         
