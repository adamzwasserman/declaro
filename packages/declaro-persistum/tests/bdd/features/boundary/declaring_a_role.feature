@bdd
Feature: Saying what a function is allowed to do
  As someone reading a function for the first time
  I want its role written on it
  So that I do not have to work out for myself whether it may open a socket

  # FOUR MARKERS, AND NOTHING HAPPENS AT RUNTIME. Each hands the function back
  # exactly as it arrived. They are declarations, and `honest-check` reads
  # them by name: a function marked a boundary may open connections, read
  # files, take the clock and catch what those raise; one without a marker may
  # not, and the checker says so.
  #
  # THE NAMES ARE THE INTERFACE, so a scenario names each of them. 972 places
  # in `src` carry one.

  @precommit
  Scenario Outline: a marker hands the function back untouched
    When "<marker>" is put on a function
    Then the same function comes back
    And it keeps its own name and docstring

    Examples:
      | marker     |
      | boundary   |
      | helper     |
      | link       |
      | recognizer |

  @precommit
  Scenario Outline: a marker works on an asynchronous function too
    When "<marker>" is put on an asynchronous function
    Then the same function comes back
    And it is still awaitable

    Examples:
      | marker     |
      | boundary   |
      | helper     |
      | link       |
      | recognizer |

  @precommit
  Scenario Outline: each marker says what the function it names may do
    When "<marker>" is asked what it declares
    Then it says "<says>"

    Examples:
      | marker     | says                                                     |
      | boundary   | where the package meets the world                         |
      | helper     | Data in, data out                                         |
      | link       | One step of handling a request                            |
      | recognizer | Reads one value and says which declared kind it is        |

  @precommit
  Scenario: the package offers these four markers and no others
    When the markers the module exports are listed
    Then they are exactly boundary, helper, link and recognizer
