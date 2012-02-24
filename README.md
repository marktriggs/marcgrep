What does it do?
----------------

MARCgrep lets you find all the records in a library collection
matching some criteria.  It searches MARC records in the dumbest way
possible: parsing them one at a time and returning any that match.

Input and output are designed to be pluggable, so it should be
possible to extend MARCgrep to read your MARC records from any source
and write them out in whatever format you like.  So far I've got it
reading records from a file or a Lucene index, and writing to either
MARC21 or plain text, but I'd be happy to lend a hand if you have
grand ideas (like reading records from your ILMS system).


What sort of searches can I do?
-------------------------------

Some examples:

  * Find records with the keyword "laugh" in the 245$a and "microform" in
    the 245$h

  * Find records with the string "laugh" in the 245$a (matches
    "laugh", "laughed", "laughing")

  * Find records where a 1XX field whose first indicator is '1'
    contains the keyword "Smith"

  * Find records containing more than one 600 field whose 008 field
    matches the regular expression "^.{35}eng"

  * Find records where the keyword "spatula" appears in any field

These sort of search criteria can be combined with the standard
AND/OR/NOT boolean operators to produce more complex queries.

Fields to match against can be specified with as much detail as is
useful.  For example, a field specification might match:

  * Any field (*)

  * Any subfield in a 2XX field (2*)

  * Any subfield in a 245 field (245)

  * Any subfield in a 245 field with a particular indicator value (245!1#)

  * Subfields a, b, c in a 245 field with a particular indicator value
    (245!1#$abc)

Don't let the syntax scare you off: the simple cases are simple (just
type the tag of the field you want to search), and there's inline
documentation for when you need something more complicated.


How slow is "slow-moving"?
--------------------------

Not as slow as you might think.  By pre-compiling your queries into
fairly efficient Java bytecode and splitting the MARC record parsing
and matching across a couple of CPUs, MARCgrep's naïve approach
actually performs pretty well.  Limiting MARCgrep to 4 cores of a 3ghz
Intel Xeon box, I can still check between ten and fifteen-thousand
records per second.


Screenshots
-----------

![screenshot](https://github.com/marktriggs/marcgrep/raw/master/screenshot.png)

Everything in MARCgrep happens on a single screen: you add one or more
jobs by completing the form at the top, then you run your jobs by
clicking the "Run all jobs" button.  MARCgrep batches jobs together so
running multiple jobs is just as fast (or slow, if you like) as
running one, so there's no harm in running lots of jobs at once.


Running MARCgrep in standalone mode
-----------------------------------

The quickest way to get running with MARCgrep is to run it straight
from the command-line, using its built in web server.  To do this:

  1.  Get Leiningen from http://github.com/technomancy/leiningen and put
      the 'lein' script somewhere in your $PATH.

  2.  From marcgrep's root directory, run 'lein uberjar'.  Lein will grab
      all required dependencies and produce a 'marcgrep-1.0.1-standalone.jar'.

  3. Copy resources/config.clj.example to resources/config.clj and
     edit as appropriate.

  4.  Run the jar from your MARCgrep directory, for example:

        java -cp resources:marcgrep-1.0.1-standalone.jar marcgrep.core

  5.  Point your browser at http://localhost:9095/


Running MARCgrep from a servlet container
-----------------------------------------

  1.  Get Leiningen from http://github.com/technomancy/leiningen and put
      the 'lein' script somewhere in your $PATH.

  2. Copy resources/config.clj.example to resources/config.clj and
     edit as appropriate.

  3.  From marcgrep's root directory, run 'lein deps' and then 'lein
      ring uberwar'.  Lein will grab all required dependencies and
      produce a 'marcgrep-1.0.1-standalone.war'.

  4.  Deploy this WAR file in your servlet container of choice (Jetty,
      Tomcat, ...)


Acknowledgements
----------------

Parts of the MARCgrep development were funded by the [National Library
of Australia](http://www.nla.gov.au/), who have generously contributed
their modifications back to the open source community.  Hurrah!

Stefano Bargioni independently spun up a project called
[MARCgrep.pl](http://en.pusc.it/bib/MARCgrep) at around the same time
I was doing this.  If you're looking for a handy way of filtering a
file of MARC records from the command line, this may be the tool for
you.
