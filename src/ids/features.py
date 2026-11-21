"""Feature transformation stages that live *inside* the saved Spark PipelineModel.

Putting imputation + log scaling in a SQLTransformer means the streaming scorer and the web
upload path only need the raw canonical numeric columns: the saved model does the rest, so batch
and streaming can never drift apart.
"""
from __future__ import annotations

from pyspark.ml.feature import SQLTransformer, StandardScaler, VectorAssembler


def _clean(c: str) -> str:
    x = f"`{c}`"
    return (f"(CASE WHEN {x} IS NULL OR isnan({x}) OR abs({x}) = double('Infinity') "
            f"THEN 0D ELSE {x} END)")


def _slog(c: str) -> str:
    # signed log1p: monotone (trees unaffected), tames the 1e9-scale byte/s features for LR,
    # and keeps legitimate -1 values (e.g. init_fwd_win_byts) distinguishable.
    x = _clean(c)
    return f"SIGN({x}) * LN(1D + ABS({x}))"


def feature_stages(feature_cols: list[str], prefix: str, scale: bool):
    """Stages turning raw numeric columns into `{prefix}vec` (and `{prefix}scaled` if scale)."""
    select = ", ".join(f"{_slog(c)} AS `{prefix}{c}`" for c in feature_cols)
    stages = [
        SQLTransformer(statement=f"SELECT *, {select} FROM __THIS__"),
        VectorAssembler(inputCols=[f"{prefix}{c}" for c in feature_cols],
                        outputCol=f"{prefix}vec", handleInvalid="keep"),
    ]
    out = f"{prefix}vec"
    if scale:
        # Fitted on the training split only (Pipeline.fit sees training data only).
        stages.append(StandardScaler(inputCol=out, outputCol=f"{prefix}scaled",
                                     withMean=True, withStd=True))
        out = f"{prefix}scaled"
    return stages, out
