
(library (dataframe display)
  (export dataframe-display
          dataframe-glimpse)

  (import (rnrs)
          (slib format)
          (dataframe record-types)
          (only (dataframe filter)
                dataframe-head)
          (only (dataframe helpers)
                add1
                sub1
                list-head
                transpose))

  ;; format directive reference:
  ;; https://www.hexstreamsoft.com/articles/common-lisp-format-reference/clhs-summary/#subsections-summary-table
  ;;
  ;; directives used in this file:
  ;;   ~d        -- print integer in decimal notation
  ;;   ~a        -- print in human-readable form (no quotes on strings/chars)
  ;;   ~Na       -- ~a with minimum field width N, padding on right (left-aligned)
  ;;   ~N@a      -- ~a with minimum field width N, padding on left (right-aligned)
  ;;   ~N,Df     -- floating-point: field width N, D digits after decimal
  ;;   ~N,D,Ee   -- exponential: field width N, D digits after decimal, E digits in exponent
  ;;   ~&        -- emit newline unless already at start of line
  ;;   ~%        -- unconditional newline
  ;;   ~{...~}   -- iterate over a flat list; body is applied repeatedly
  ;;   ~:{...~}  -- iterate over a list of sublists; each sublist is one iteration's arguments

  ;; ----------------------------------------
  ;; dataframe-glimpse
  ;; Prints a compact summary: one row per column showing name, type, and a
  ;; truncated list of values. Default total-width is 76 characters.
  ;; ----------------------------------------

  (define dataframe-glimpse
    (case-lambda
      [(df) (df-glimpse-helper df 76)]
      [(df total-width) (df-glimpse-helper df total-width)]))

  (define (df-glimpse-helper df total-width)
    (check-dataframe df "(dataframe-glimpse df)")
    (let* ([slist (dataframe-slist df)]
           [df-names (dataframe-names df)]
           [df-types (prepare-df-types df)]     ;; types as strings, e.g. "<num>"
           [dim (dataframe-dim df)]             ;; (rows . cols)
           [rows (car dim)]
           ;; cap at 500 rows to limit work; enough to compute display widths accurately
           [n-actual (if (< rows 500) rows 500)]
           [ls-vals (map (lambda (series) (list-head (series-lst series) n-actual)) slist)]
           [name-width (get-width df-names 7)]  ;; max name width, at least 7
           [type-width (get-width df-types 7)]  ;; max type width, at least 7
           [list-width (- total-width (+ name-width type-width))]  ;; remaining width for values
           [gfs (glimpse-format-string name-width type-width list-width)]
           ;; convert each column's value list to a truncated display string
           [list-str (map (lambda (x) (prepare-lst x list-width)) ls-vals)])
      (format #t " dim: ~d rows x ~d cols" (car dim) (cdr dim))
      ;; gfs uses ~:{...~} so it expects a list of sublists; build-format-list provides that
      (format #t gfs (build-format-list df-names df-types list-str))
      (newline)))

  ;; Zip names, types, and value-strings into a list of (name type values) triples,
  ;; suitable for ~:{...~} iteration in glimpse-format-string.
  (define (build-format-list df-names df-types list-str)
    (map (lambda (n t ls) (list n t ls)) df-names df-types list-str))

  ;; Return the maximum display width of items in lst (with pad=2), enforcing a minimum.
  ;; Used for glimpse to size the name and type columns.
  (define (get-width lst min-width)
    (let ([mx (apply max (map (lambda (x) (compute-object-width x 2)) lst))])
      (if (> mx min-width) mx min-width)))

  ;; Build the format string for one glimpse row, e.g.:
  ;;   "~:{~& ~8a ~7a ~40a ~}"
  ;; Each iteration prints: name (left-aligned), type (left-aligned), value list (left-aligned).
  ;; ~a (no @) is used here because glimpse columns are left-aligned.
  (define (glimpse-format-string name-width type-width list-width)
    (let* ([nw (number->string name-width)]
           [tw (number->string type-width)]
           [lw (number->string list-width)])
      (string-append "~:{~& ~" nw "a ~" tw "a ~" lw "a ~}")))

  ;; Convert a column's value list to a single display string truncated to lst-width characters.
  ;; Values are separated by ", "; the string ends with ", ..." if truncated.
  (define (prepare-lst lst lst-width)
    ;; prepare-non-numbers replaces nested structures (lists, vectors, etc.)
    ;; with placeholder strings like <list>, <vector>
    (let loop ([lst (prepare-non-numbers lst)]
               [used-width 1]  ;; starts at 1 to account for the opening space
               [out-str ""])
      (cond [(null? lst)
             out-str]
            ;; check if adding the next value (plus ", ...") would exceed the width
            [(>= (+ used-width 5 (string-length (->string (car lst)))) lst-width)
             (string-append out-str ", ...")]
            [else
             (let* ([fill-str (if (= used-width 1) "" ", ")]  ;; no leading ", " for first item
                    [str (string-append fill-str (->string (car lst)))]
                    [str-width (string-length str)])
               (loop (cdr lst) (+ used-width str-width) (string-append out-str str)))])))

  ;; Convert any scalar value to its string representation for display purposes.
  (define (->string obj)
    (cond [(boolean? obj) (if obj "#t" "#f")]
          [(number? obj) (number->string obj)]
          [(symbol? obj) (symbol->string obj)]
          [(char? obj) (string obj)]
          ;; only other thing passed would be a string
          [else obj]))

  ;; ----------------------------------------
  ;; dataframe-display
  ;; Prints a formatted table: dim header, column names, column types, and n rows of values.
  ;; Columns that don't fit within total-width are omitted and listed in a footer.
  ;; Defaults: n=10 rows, total-width=76, min column width=7.
  ;; ----------------------------------------

  (define dataframe-display
    (case-lambda
      [(df) (df-display-helper df 10 76 7)]
      [(df n) (df-display-helper df n 76 7)]
      [(df n total-width) (df-display-helper df n total-width 7)]
      [(df n total-width min-width) (df-display-helper df n total-width min-width)]))

  (define (df-display-helper df n total-width min-width)
    (let ([who "(dataframe-display df)"])
      (check-dataframe df who)
      (unless (> total-width min-width)
        (assertion-violation who "total-width must be greater than min-width")))
    (let* ([slist (dataframe-slist df)]
           [df-names (dataframe-names df)]
           [df-types (prepare-df-types df)]
           [dim (dataframe-dim df)]
           [rows (car dim)]
           [n-actual (if (< rows n) rows n)]  ;; don't request more rows than exist
           [ls-vals (map (lambda (series) (list-head (series-lst series) n-actual)) slist)])
      (format-df df-names df-types ls-vals dim total-width min-width)))

  ;; Orchestrates display output: prints dim, header, types, table, and footer.
  ;; Each of header/types/table is an association list entry: (label format-string values).
  (define (format-df df-names df-types ls-vals dim total-width min-width)
    (let* ([prep-vals (map prepare-non-numbers ls-vals)]
           [parts (build-format-parts df-names df-types prep-vals total-width min-width 2)])
      (format #t " dim: ~d rows x ~d cols" (car dim) (cdr dim))
      ;; each parts entry is (label format-string values); cadr=format-string, caddr=values
      (format #t (cadr (assoc 'header parts)) (caddr (assoc 'header parts)))
      (format #t (cadr (assoc 'types parts))  (caddr (assoc 'types parts)))
      (format #t (cadr (assoc 'table parts))  (caddr (assoc 'table parts)))
      (newline)
      (display (cdr (assoc 'footer parts)))))

  ;; Return a list of type strings wrapped in angle brackets, e.g. "<num>", "<str>".
  (define (prepare-df-types df)
    (let ([df-types (map series-type (dataframe-slist df))])
      (map (lambda (x) (string-append "<" (symbol->string x) ">")) df-types)))

  ;; For numeric-only columns, return the list unchanged (numbers format via ~f/~e).
  ;; For mixed or non-numeric columns, replace compound objects with placeholder strings
  ;; like "<list>" or "<vector>"; leave booleans, chars, strings, symbols as-is.
  (define (prepare-non-numbers lst)
    (let ([types (map get-display-type lst)])
      (if (for-all number? lst)
          lst
          (map (lambda (obj typ) (get-display-value obj typ)) lst types))))

  ;; Map each object to a type symbol used by get-display-value.
  ;; null? must precede list? because null is also a list.
  ;; integer?, exact-number?, number? order matters: more specific predicates first.
  (define (get-display-type object)
    (let loop ([preds (list boolean? char? integer? exact-number? number? string?
                            symbol? null? list? pair? vector? dataframe? hashtable?)]
               [types '(boolean char integer exact number string
                                symbol null list pair vector dataframe hashtable)])
      (if (null? preds)
          'other
          (if ((car preds) object)
              (car types)
              (loop (cdr preds) (cdr types))))))

  (define (exact-number? object)
    (and (number? object) (exact? object)))

  ;; Return the display value for an object in a non-numeric column:
  ;;   - primitive types (boolean, char, integer, etc.) pass through unchanged
  ;;   - compound/unknown types become placeholder strings like "<list>", "<other type>"
  (define (get-display-value object display-type)
    (cond [(member display-type '(boolean char integer exact number string symbol)) object]
          [(symbol=? display-type 'other) "<other type>"]
          [else (string-append "<" (symbol->string display-type) ">")]))

  ;; ----------------------------------------
  ;; build-format-parts
  ;;
  ;; Core layout engine for dataframe-display. Iterates over columns left to right,
  ;; accumulating format strings and values until adding the next column would exceed
  ;; total-width. Remaining columns are passed to format-footer.
  ;;
  ;; Returns an alist with keys: header, types, table, footer.
  ;; Each of header/types/table maps to (format-string values).
  ;; footer maps to a string listing omitted column names.
  ;;
  ;; Example output:
  ;;   ((header "~& ~{~8@a ~7@a ~8@a ~}"  (Boolean Char String))
  ;;    (types  "~& ~{~8@a ~7@a ~8@a ~}"  ("<bool>" "<chr>" "<str>"))
  ;;    (table  "~:{~& ~8@a ~7@a ~8@a ~}" ((#t #\y "these") (#f #\e "are")))
  ;;    (footer . " Columns not displayed: Dec2, Other\n"))
  ;;
  ;; Format directive notes:
  ;;   ~N@a       -- right-align in field of width N (used for header/types/non-numeric table cols)
  ;;   ~N,Df      -- floating-point in field of width N with D decimal places
  ;;   ~N,D,Ee    -- exponential in field of width N, D decimals, E exponent digits
  ;;   ~{...~}    -- iterate over flat list (header and types rows)
  ;;   ~:{...~}   -- iterate over list of sublists (table rows, one sublist per row)
  ;; ----------------------------------------

  (define (build-format-parts names df-types prep-vals total-width min-width pad)
    (let* ([e-dec 3]  ;; number of decimal digits used when displaying in exponential notation
           ;; compute formatting metadata (num-type, width, decimal, esigfig) per column
           [format-parts (map (lambda (lst)
                                (compute-format-parts lst e-dec pad)) prep-vals)]
           [val-widths  (map-efp format-parts 'width)]
           ;; final column width = max of name width, type width, value width, and min-width
           [col-widths  (compute-column-widths names df-types val-widths min-width pad)]
           [num-types   (map-efp format-parts 'num-type)]   ;; "f", "e", or "nan" (non-numeric)
           [declst      (map-efp format-parts 'decimal)]    ;; decimal places per column
           [esiglst     (map-efp format-parts 'esigfig)])   ;; exponent sig figs per column
      (let loop ([names names]
                 [df-types df-types]
                 [prep-vals prep-vals]
                 [nt num-types]
                 [cw col-widths]
                 [dec declst]
                 [esig esiglst]
                 [used-width 0]
                 [used-names '()]
                 [used-types '()]
                 [used-vals  '()]
                 ;; format strings are built up incrementally as columns are added
                 [hd  "~& ~{"]   ;; header row:  ~{...~} iterates flat list of names
                 [typ "~& ~{"]   ;; types row:   ~{...~} iterates flat list of type strings
                 [tbl "~:{~& "]) ;; table rows:  ~:{...~} iterates list of row sublists
        (if (or (null? names) (>= (+ used-width (car cw)) total-width))
            ;; base case: no more columns fit; close format strings and package results
            (list (cons 'header (list (string-append hd "~}")
                                      (reverse used-names)))
                  (cons 'types  (list (string-append typ "~}")
                                      (reverse used-types)))
                  (cons 'table  (list (string-append tbl "~}")
                                      ;; transpose converts list-of-columns to list-of-rows
                                      (transpose (reverse used-vals))))
                  (cons 'footer (format-footer names)))
            ;; recursive case: append this column's directive to each format string
            (let* ([width-part (string-append "~" (number->string (car cw)))]
                   ;; header/types always use ~N@a (right-aligned, human-readable)
                   [hdr-part   (string-append width-part "@a ")]
                   ;; table uses ~N@a for non-numeric columns, ~N,Df or ~N,D,Ee for numeric
                   [full-part  (if (string=? (car nt) "nan")
                                   hdr-part
                                   (string-append
                                    width-part
                                    (format-number (car nt) (car dec) (car esig))))])
              (loop (cdr names)
                    (cdr df-types)
                    (cdr prep-vals)
                    (cdr nt)
                    (cdr cw)
                    (cdr dec)
                    (cdr esig)
                    (+ used-width (car cw))
                    (cons (car names)     used-names)
                    (cons (car df-types)  used-types)
                    (cons (car prep-vals) used-vals)
                    ;; header and types share the same per-column directive (~N@a)
                    (string-append hd  hdr-part)
                    (string-append typ hdr-part)
                    (string-append tbl full-part)))))))

  ;; Compute formatting metadata for one column's value list.
  ;; For non-numeric / fraction columns: returns just (width . W).
  ;; For integer/float columns: returns (num-type . "f"|"e"), (width . W),
  ;;   (decimal . D), (esigfig . E).
  ;; If any value is an exact fraction (not integer, not inexact), the whole column
  ;; is treated as non-numeric and routed through compute-object-width.
  (define (compute-format-parts lst e-dec pad)
    (if (not (for-all not-fraction-number? lst))
        ;; column contains exact fractions: treat as non-numeric
        (let ([widthlst (map (lambda (x) (compute-object-width x pad)) lst)])
          (list (cons 'width (apply max widthlst))))
        ;; column is integer or inexact: compute numeric formatting
        (let* ([neg     (if (any-negative? lst) 1 0)]  ;; 1 if we need space for a minus sign
               [lst     (map abs lst)]
               [siglst  (map compute-sigfig lst)]       ;; digits left of decimal point
               [elst    (map compute-expt lst)]         ;; base-10 exponent of each value
               [esig    (apply max (map compute-sigfig elst))]  ;; max exponent digits needed
               [declst  (map (lambda (x sig e)
                               (compute-decimal x sig e e-dec))
                             lst siglst elst)]
               ;; if any value gets e-dec decimals, use exponential notation for the whole column
               [num-type (if (> (length (filter (lambda (x) (= x e-dec)) declst)) 0) "e" "f")]
               [widthlst (map (lambda (sig dec)
                                (compute-num-width num-type neg sig esig dec pad))
                              siglst declst)]
               ;; for "e" columns all values use e-dec; for "f" use the max decimal places needed
               [dec (if (string=? num-type "e") e-dec (apply max declst))])
          (list (cons 'num-type num-type)
                (cons 'width    (apply max widthlst))
                (cons 'decimal  dec)
                (cons 'esigfig  esig)))))

  ;; Returns #t for integers and inexact numbers (the kinds compute-num-width can handle).
  ;; Returns #f for exact non-integer rationals (fractions like 1/3), which need
  ;; compute-object-width instead because their string representation is unpredictable.
  (define (not-fraction-number? object)
    (or (integer? object)
        (and (number? object) (not (exact? object)))))

  ;; Compute the formatted field width for a number in a numeric-only column.
  ;; For "e": neg + esig + dec + pad + 3  (the 3 covers "e+N" or "e-N" notation overhead)
  ;; For "f": neg + sig + dec + pad
  (define (compute-num-width num-type neg sig esig dec pad)
    (if (string=? num-type "e")
        (+ neg esig dec pad 3)
        (+ neg sig dec pad)))

  ;; Compute the display width for a single non-numeric object (or exact fraction).
  ;; Used for mixed-type columns and for sizing name/type header columns.
  ;; The sub1 corrects for an observed excess space that arises from the pad arithmetic.
  (define (compute-object-width object pad)
    (let ([obj-width
           (cond [(boolean? object)
                  (string-length (if object "#t" "#f"))]
                 [(char? object)
                  (string-length (string object))]
                 [(number? object)
                  (string-length (number->string object))]
                 [(string? object)
                  (string-length object)]
                 [(symbol? object)
                  (string-length (symbol->string object))]
                 [else
                  (assertion-violation
                   "(compute-width object pad)"
                   "object of this type was not anticipated")])])
      (sub1 (+ pad obj-width))))

  ;; Apply extract-format-parts across all columns. "efp" = extract format parts.
  (define (map-efp format-parts part)
    (map (lambda (lst) (extract-format-parts lst part)) format-parts))

  ;; Extract a single field (e.g. 'width, 'num-type) from one column's format-parts alist.
  ;; Returns "nan" for number-specific fields (decimal, esigfig) when the column is non-numeric,
  ;; signalling to build-format-parts that ~N@a should be used instead of ~N,Df / ~N,D,Ee.
  (define (extract-format-parts lst part)
    (if (and (not (symbol=? part 'width))
             (not (assoc 'num-type lst)))
        "nan"
        (cdr (assoc part lst))))

  ;; Determine the display width for each column as the maximum of:
  ;; the column name width, the type string width, the value width, and min-width.
  (define (compute-column-widths names df-types val-widths min-width pad)
    (let ([name-widths
           (map (lambda (name) (compute-object-width name pad)) names)]
          [type-widths
           (map (lambda (df-type) (compute-object-width df-type pad)) df-types)])
      (map (lambda (name-width type-width val-width)
             (max min-width name-width type-width val-width))
           name-widths type-widths val-widths)))

  (define (any-negative? lst)
    (> (length (filter negative? lst)) 0))

  ;; Build the numeric portion of a format directive (everything after "~N").
  ;; For "f": returns ",Df "   e.g. ",4f "
  ;; For "e": returns ",D,Ee "  e.g. ",3,1e "
  (define (format-number num-type dec esig)
    (let ([dec-part (string-append "," (number->string dec))])
      (if (string=? num-type "f")
          (string-append dec-part "f ")
          (string-append dec-part "," (number->string esig) "e "))))

  ;; Determine how many decimal places to show for a number.
  ;; Returns e-dec (the exponential threshold) to signal that exponential notation should be used.
  ;; Rules:
  ;;   very large or very small (|e| > 5 or < -3) -> exponential notation
  ;;   integer                                     -> 0 decimals
  ;;   large (e > 3)                               -> 2 decimals (less precision needed)
  ;;   small fraction with many sig figs           -> use sigfig count
  ;;   default                                     -> 4 decimals
  (define (compute-decimal x sigfig e e-dec)
    (let ([default 4]
          [x (abs x)])
      (cond [(or (< e -3) (> e 5)) e-dec]
            [(integer? x) 0]
            [(> e 3) 2]
            [(and (< x 1) (> sigfig default)) sigfig]
            [else default])))

  ;; Compute the base-10 exponent (order of magnitude) of x.
  ;; Adapted from https://github.com/r-lib/pillar/blob/master/R/sigfig.R
  ;; A small offset is applied to avoid boundary errors (e.g. log10(1000) = 2.9999...).
  (define (compute-expt x)
    (let* ([x      (abs x)]
           [digits 4]
           [offset (/ (log (add1 (* -5 (expt 10 (sub1 (* -1 digits)))))) (log 10))])
      (if (zero? x)
          0
          (exact
           (floor (- (log10 x) offset))))))

  ;; Count the digits to the left of the decimal point (i.e. floor(log10(x)) + 1).
  ;; Returns 1 for zero.
  (define (compute-sigfig x)
    (let ([x (abs x)])
      (if (zero? x)
          1
          (exact
           (add1 (floor (log10 x)))))))

  ;; A numerically stable log base 10.
  ;; (log 1000 10) can return 2.9999... due to floating-point error;
  ;; this rounds to the nearest integer when the result is within eps of one.
  (define (log10 x)
    (let* ([eps  1e-10]
           [logx (log x 10)]
           [rlx  (round logx)])
      (if (and (> rlx (- logx eps))
               (< rlx (+ logx eps)))
          rlx
          logx)))

  ;; Build the footer string listing column names that were omitted due to width constraints.
  ;; Returns "" if all columns fit.
  (define (format-footer names)
    (if (null? names)
        ""
        (string-append " Columns not displayed: " (add-commas names) "\n")))

  ;; Join a list of symbols as a comma-separated string, e.g. '(a b c) -> "a, b, c".
  (define (add-commas ls)
    (let ([ls (map symbol->string ls)])
      (apply string-append
             (let loop ([ls      (cdr ls)]
                        [results (list (car ls))])
               (if (null? ls)
                   (reverse results)
                   (loop (cdr ls) (cons (car ls) (cons ", " results))))))))

  )
