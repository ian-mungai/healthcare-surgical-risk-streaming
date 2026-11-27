"""One feature code for batch and stream: a window of readings in, a row of raw feature values out.

The entry point is ``features.row.build_row``. The batch entry point (``features.batch``) and the streaming job call it and nothing else. The package holds no
fitted step: it returns raw values, and a value that cannot be known is ``None``. It uses the standard library only, so the batch and stream images share it.
"""
