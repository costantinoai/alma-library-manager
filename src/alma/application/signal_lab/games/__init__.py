"""Signal Lab minigames — pure data + one pure ``interpret`` each.

D20 guard (``tests/test_signal_lab_layer_contract.py``): no module in this
package may import ``sqlite3``, annotate a ``Connection``, or call
``.execute(`` — a game that needs the database is a design error, not a
bigger game. Register new games in ``signal_lab.available_games()``.
"""
