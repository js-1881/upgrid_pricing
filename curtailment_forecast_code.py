import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import joblib
import json
import warnings
import os

from sklearn.model_selection import TimeSeriesSplit, RandomizedSearchCV
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    roc_auc_score,
    average_precision_score,
    confusion_matrix,
    precision_recall_curve,
    roc_curve,
    r2_score,
    mean_squared_error,
    mean_absolute_error,
    mean_absolute_percentage_error,
)
from xgboost import XGBClassifier, XGBRegressor
from matplotlib.ticker import MaxNLocator, FuncFormatter
from scipy.stats import randint, uniform

warnings.filterwarnings("ignore")

# =============================================================================
# SMALL HELPERS
# =============================================================================


def print_header(title: str):
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def convert_to_serializable(obj):
    """Make nested structures JSON/joblib-friendly."""
    if isinstance(obj, (np.float32, np.float64)):
        return float(obj)
    if isinstance(obj, (np.int32, np.int64)):
        return int(obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, dict):
        return {k: convert_to_serializable(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [convert_to_serializable(v) for v in obj]
    return obj


def validate_required_columns(df: pd.DataFrame, required_columns, context: str):
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"{context} is missing required columns: {missing}")


# TimeSeriesSplit requires at least n_splits + 1 samples. This helper ensures we don't request more splits than possible.
def get_safe_n_splits(
    n_samples: int, requested_splits: int, min_splits: int = 2
) -> int:
    if n_samples < min_splits + 1:
        raise ValueError(
            f"Not enough samples ({n_samples}) for TimeSeriesSplit with at least {min_splits} splits."
        )
    max_splits = n_samples - 1
    return max(min_splits, min(requested_splits, max_splits))


def choose_optimal_threshold(y_true, y_proba, default_threshold: float = 0.5) -> float:
    precision_vals, recall_vals, thresholds = precision_recall_curve(y_true, y_proba)
    if thresholds.size == 0:
        return default_threshold
    f1_vals = (
        2
        * (precision_vals[1:] * recall_vals[1:])
        / (precision_vals[1:] + recall_vals[1:] + 1e-8)
    )
    f1_vals = np.nan_to_num(f1_vals, nan=0.0)
    best_idx = int(np.argmax(f1_vals))
    return float(thresholds[best_idx])


def safe_roc_auc_score(y_true, y_proba):
    return roc_auc_score(y_true, y_proba) if pd.Series(y_true).nunique() > 1 else np.nan


def safe_average_precision_score(y_true, y_proba):
    return (
        average_precision_score(y_true, y_proba)
        if pd.Series(y_true).nunique() > 1
        else np.nan
    )


def ensure_parent_dir(path: str):
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def cleanup_files(file_paths):
    for path in file_paths:
        if path and os.path.exists(path):
            os.remove(path)


# =============================================================================
# CLASSIFICATION – PLOTS
# =============================================================================


def plot_fold_results(y_test, y_pred, y_pred_proba, model, features, threshold, fold):
    """Per-fold diagnostic plots for the classifier."""
    fig, axes = plt.subplots(2, 3, figsize=(18, 12))
    fig.suptitle(
        f"Fold {fold} - Classification Results", fontsize=16, fontweight="bold"
    )

    # 1. Confusion Matrix
    cm = confusion_matrix(y_test, y_pred)
    sns.heatmap(
        cm,
        annot=True,
        fmt="d",
        cmap="Blues",
        ax=axes[0, 0],
        xticklabels=["No Curtailment", "Curtailment"],
        yticklabels=["No Curtailment", "Curtailment"],
    )
    axes[0, 0].set_title("Confusion Matrix")
    axes[0, 0].set_xlabel("Predicted")
    axes[0, 0].set_ylabel("Actual")

    # 2. ROC Curve
    fpr, tpr, _ = roc_curve(y_test, y_pred_proba)
    roc_auc = roc_auc_score(y_test, y_pred_proba)
    axes[0, 1].plot(
        fpr, tpr, color="darkorange", lw=2, label=f"ROC (AUC = {roc_auc:.3f})"
    )
    axes[0, 1].plot([0, 1], [0, 1], color="navy", lw=2, linestyle="--")
    axes[0, 1].set_xlim([0.0, 1.0])
    axes[0, 1].set_ylim([0.0, 1.05])
    axes[0, 1].set_xlabel("False Positive Rate")
    axes[0, 1].set_ylabel("True Positive Rate")
    axes[0, 1].set_title("ROC Curve")
    axes[0, 1].legend()
    axes[0, 1].grid(True, alpha=0.3)

    # 3. Precision-Recall Curve
    precision_vals, recall_vals, _ = precision_recall_curve(y_test, y_pred_proba)
    avg_precision = average_precision_score(y_test, y_pred_proba)
    axes[0, 2].plot(
        recall_vals,
        precision_vals,
        color="blue",
        lw=2,
        label=f"Avg Precision = {avg_precision:.3f}",
    )
    axes[0, 2].set_xlim([0.0, 1.0])
    axes[0, 2].set_ylim([0.0, 1.05])
    axes[0, 2].set_xlabel("Recall")
    axes[0, 2].set_ylabel("Precision")
    axes[0, 2].set_title("Precision-Recall Curve")
    axes[0, 2].legend()
    axes[0, 2].grid(True, alpha=0.3)

    # 4. Probability Distribution
    axes[1, 0].hist(
        y_pred_proba[y_test == 0],
        bins=30,
        alpha=0.7,
        label="No Curtailment",
        color="red",
        density=True,
    )
    axes[1, 0].hist(
        y_pred_proba[y_test == 1],
        bins=30,
        alpha=0.7,
        label="Curtailment",
        color="blue",
        density=True,
    )
    axes[1, 0].axvline(
        threshold, color="black", linestyle="--", label=f"Threshold: {threshold:.3f}"
    )
    axes[1, 0].set_xlabel("Predicted Probability")
    axes[1, 0].set_ylabel("Density")
    axes[1, 0].set_title("Probability Distribution by Class")
    axes[1, 0].legend()
    axes[1, 0].grid(True, alpha=0.3)

    # 5. Feature Importance
    importance = model.feature_importances_
    indices = np.argsort(importance)[-10:]  # Top 10
    axes[1, 1].barh(range(len(indices)), importance[indices])
    axes[1, 1].set_yticks(range(len(indices)))
    axes[1, 1].set_yticklabels([features[i] for i in indices])
    axes[1, 1].set_xlabel("Feature Importance")
    axes[1, 1].set_title("Top 10 Feature Importance")

    # 6. Threshold Analysis
    thresholds = np.arange(0.1, 1.0, 0.05)
    precisions, recalls, f1_scores = [], [], []

    for thresh in thresholds:
        y_pred_thresh = (y_pred_proba >= thresh).astype(int)
        precisions.append(precision_score(y_test, y_pred_thresh, zero_division=0))
        recalls.append(recall_score(y_test, y_pred_thresh, zero_division=0))
        f1_scores.append(f1_score(y_test, y_pred_thresh, zero_division=0))

    axes[1, 2].plot(thresholds, precisions, "b-", label="Precision", linewidth=2)
    axes[1, 2].plot(thresholds, recalls, "g-", label="Recall", linewidth=2)
    axes[1, 2].plot(thresholds, f1_scores, "r-", label="F1-Score", linewidth=2)
    axes[1, 2].axvline(
        threshold,
        color="black",
        linestyle="--",
        label=f"Optimal Threshold: {threshold:.3f}",
    )
    axes[1, 2].set_xlabel("Threshold")
    axes[1, 2].set_ylabel("Score")
    axes[1, 2].set_title("Metrics vs Threshold")
    axes[1, 2].legend()
    axes[1, 2].grid(True, alpha=0.3)

    plt.tight_layout()
    plt.show()


def plot_comprehensive_summary(metrics_df, feature_importance_df, X, y):
    """Summary plots across all classification folds."""
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle("Comprehensive Classification Summary", fontsize=16, fontweight="bold")

    # 1. Metrics across folds
    metric_names = ["accuracy", "precision", "recall", "f1"]
    fold_metrics = metrics_df[["fold"] + [f"optimal_{m}" for m in metric_names]]

    x = np.arange(len(fold_metrics["fold"]))
    width = 0.18

    for i, metric in enumerate(metric_names):
        axes[0, 0].bar(
            x + i * width,
            fold_metrics[f"optimal_{metric}"],
            width=width,
            label=metric.capitalize(),
        )

    axes[0, 0].set_xlabel("Fold")
    axes[0, 0].set_ylabel("Score")
    axes[0, 0].set_title("Metrics Across Folds")
    axes[0, 0].set_xticks(x + 1.5 * width)
    axes[0, 0].set_xticklabels([f"Fold {f}" for f in fold_metrics["fold"]])
    axes[0, 0].legend()
    axes[0, 0].grid(True, alpha=0.3)

    # 2. Feature Importance
    top_features = feature_importance_df.head(10)
    axes[0, 1].barh(range(len(top_features)), top_features["importance"])
    axes[0, 1].set_yticks(range(len(top_features)))
    axes[0, 1].set_yticklabels(top_features["feature"])
    axes[0, 1].set_xlabel("Average Importance")
    axes[0, 1].set_title("Top 10 Feature Importance (Avg Across Folds)")

    # 3. Class Distribution
    class_counts = y.value_counts()
    axes[1, 0].pie(
        class_counts.values,
        labels=["No Curtailment", "Curtailment"],
        autopct="%1.1f%%",
        startangle=90,
        colors=["lightblue", "lightcoral"],
    )
    axes[1, 0].set_title(f"Overall Class Distribution\nTotal samples: {len(y):,}")

    # 4. Correlation Heatmap (top features)
    top_feature_names = top_features["feature"].head(8).tolist()
    if len(top_feature_names) > 1:
        corr_matrix = X[top_feature_names].corr()
        im = axes[1, 1].imshow(
            corr_matrix, cmap="coolwarm", aspect="auto", vmin=-1, vmax=1
        )
        axes[1, 1].set_xticks(range(len(top_feature_names)))
        axes[1, 1].set_yticks(range(len(top_feature_names)))
        axes[1, 1].set_xticklabels(top_feature_names, rotation=45, ha="right")
        axes[1, 1].set_yticklabels(top_feature_names)
        axes[1, 1].set_title("Feature Correlation Heatmap (Top 8 Features)")
        for i in range(len(top_feature_names)):
            for j in range(len(top_feature_names)):
                axes[1, 1].text(
                    j,
                    i,
                    f"{corr_matrix.iloc[i, j]:.2f}",
                    ha="center",
                    va="center",
                    fontsize=8,
                )

    plt.tight_layout()
    plt.show()

    print_header("FINAL CLASSIFICATION PERFORMANCE SUMMARY")
    optimal_metrics = [
        c
        for c in metrics_df.columns
        if c.startswith("optimal_") and c != "optimal_threshold"
    ]
    for metric in optimal_metrics:
        metric_name = metric.replace("optimal_", "").replace("_", " ").title()
        mean_val = metrics_df[metric].mean()
        std_val = metrics_df[metric].std()
        print(f"{metric_name:<20}: {mean_val:.4f} ± {std_val:.4f}")


# =============================================================================
# CLASSIFICATION – MAIN FUNCTION
# =============================================================================


def comprehensive_classification_analysis(
    df,
    test_size_final: float = 0.2,
    n_splits: int = 3,
    n_iter: int = 50,
    save_model: bool = True,
    plot_folds: bool = True,
):
    """
    Curtailment classification with:
    - Feature engineering
    - TimeSeries CV + RandomizedSearch
    - Per-fold diagnostics
    - Final 80/20 chronological test
    """
    print_header("CURTAILMENT CLASSIFICATION ANALYSIS")

    df = df.copy()
    validate_required_columns(df, ["curtailment_kWh_per_kw"], "Classification input")

    if "volume__mw_imbalance" not in df.columns:
        df["volume__mw_imbalance"] = 0

    if "dayaheadprice_eur_mwh" not in df.columns:
        df["dayaheadprice_eur_mwh"] = np.nan
    if "rebap_euro_per_mwh" not in df.columns:
        df["rebap_euro_per_mwh"] = np.nan

    df["curtailment_flag"] = (df["curtailment_kWh_per_kw"] > 0).astype(int)
    df["volume__mw_imbalance"] = pd.to_numeric(
        df["volume__mw_imbalance"], errors="coerce"
    ).fillna(0)

    # Price flags
    df["dayaheadprice_eur_mwh"] = pd.to_numeric(
        df["dayaheadprice_eur_mwh"], errors="coerce"
    )
    df["rebap_euro_per_mwh"] = pd.to_numeric(df["rebap_euro_per_mwh"], errors="coerce")
    df["DA_negative_flag"] = (df["dayaheadprice_eur_mwh"] < 0).astype(int)
    df["DA_negative_flag_lag_1"] = df["DA_negative_flag"].shift(1)
    df["rebap_negative_flag"] = (df["rebap_euro_per_mwh"] < 0).astype(int)
    df["rebap_negative_flag_lag_1"] = df["rebap_negative_flag"].shift(1)

    classification_features = [
        "DA_negative_flag",
        "quarterly_energy_kWh_per_kw",
        "enwex_percentage",
        "dayaheadprice_eur_mwh",
        "rebap_euro_per_mwh",
        "volume__mw_imbalance",
        "id500_eur_mwh",
        "rmv_eur_per_mwh",
    ]

    classification_features = [f for f in classification_features if f in df.columns]
    if not classification_features:
        raise ValueError("No valid classification features found in dataframe.")

    for col in classification_features:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").ffill().bfill()

    print("Using features:")
    for f in classification_features:
        print(f"  - {f}")

    df_clean = df.dropna(subset=classification_features + ["curtailment_flag"]).copy()
    if df_clean.empty:
        raise ValueError("No rows left for classification after cleaning.")

    X = df_clean[classification_features]
    y = df_clean["curtailment_flag"]

    print(f"\nData after cleaning: {len(df_clean)} rows")
    class_dist = y.value_counts()
    print(f"  No Curtailment: {class_dist.get(0, 0):,}")
    print(f"  Curtailment:    {class_dist.get(1, 0):,}")
    print(f"  Pos class ratio: {class_dist.get(1, 0) / max(1, len(y)):.4f}")

    corr_with_target = X.corrwith(y).abs().sort_values(ascending=False)
    print("\nTop 10 correlated features with target:")
    for feat, corr in corr_with_target.head(10).items():
        print(f"  {feat}: {corr:.4f}")

    # CLASSIFICATION PARAMETERS
    param_distributions = {
        "n_estimators": [500, 750, 1000, 1500, 2000, 2500, 3000],
        "learning_rate": [0.01, 0.05, 0.1, 0.15, 0.2],
        "max_depth": [4, 5, 6, 7, 9, 11],
        "subsample": [0.6, 0.7, 0.9, 1.0],
        "colsample_bytree": [0.6, 0.7, 0.8, 0.9, 1.0],
        "gamma": [0, 0.1, 0.2, 0.3, 0.4],
        "min_child_weight": [1, 2, 3, 4, 5],
    }

    # TimeSeries CV
    tscv = TimeSeriesSplit(n_splits=get_safe_n_splits(len(X), n_splits))
    fold_metrics = []
    feature_importances = []
    optimal_thresholds = []
    best_models = []

    for fold, (train_idx, test_idx) in enumerate(tscv.split(X), 1):
        print_header(f"CLASSIFICATION FOLD {fold}/{n_splits}")

        X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
        y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]

        train_size = len(X_train)
        if train_size < 4:
            print(f"Skipping fold {fold}: not enough training rows ({train_size}).")
            continue
        split_idx = int(train_size * 0.75)
        split_idx = max(1, min(split_idx, train_size - 1))

        X_train_split, X_valid_split = X_train[:split_idx], X_train[split_idx:]
        y_train_split, y_valid_split = y_train[:split_idx], y_train[split_idx:]

        if y_train_split.nunique() < 2:
            print(f"Skipping fold {fold}: training split has only one class.")
            continue

        inner_splits = get_safe_n_splits(len(X_train_split), min(3, n_splits))

        print(
            f"Train size: {len(X_train_split)}, Validation size: {len(X_valid_split)}"
        )
        print(f"Test size: {len(X_test)}")

        neg, pos = (y_train_split == 0).sum(), (y_train_split == 1).sum()
        scale_pos_weight = neg / pos if pos > 0 else 1.0
        print(
            f"Class balance (train) 0:1 = {neg}:{pos} -> scale_pos_weight={scale_pos_weight:.2f}"
        )

        base_clf = XGBClassifier(
            scale_pos_weight=scale_pos_weight,
            eval_metric="aucpr",
            max_delta_step=1,
            random_state=42,
            n_jobs=-1,
        )

        search = RandomizedSearchCV(
            estimator=base_clf,
            param_distributions=param_distributions,
            n_iter=n_iter,
            cv=TimeSeriesSplit(n_splits=inner_splits),
            scoring="average_precision",
            n_jobs=-1,
            random_state=42,
            verbose=0,
            refit=True,
        )

        search.fit(X_train_split, y_train_split)
        clf = search.best_estimator_
        best_models.append(
            {
                "model": clf,
                "params": search.best_params_,
                "score": search.best_score_,
                "fold": fold,
            }
        )

        print(f"Best CV CLASSIFICATION: {search.best_score_:.4f}")
        for param, value in search.best_params_.items():
            print(f" 🛸 {param}: {value}")

        y_proba = clf.predict_proba(X_valid_split)[:, 1]

        opt_threshold = choose_optimal_threshold(
            y_valid_split, y_proba, default_threshold=0.5
        )
        optimal_thresholds.append(opt_threshold)

        y_pred_opt = (y_proba >= opt_threshold).astype(int)

        metrics_opt = {
            "accuracy": accuracy_score(y_valid_split, y_pred_opt),
            "precision": precision_score(y_valid_split, y_pred_opt, zero_division=0),
            "recall": recall_score(y_valid_split, y_pred_opt, zero_division=0),
            "f1": f1_score(y_valid_split, y_pred_opt, zero_division=0),
            "roc_auc": safe_roc_auc_score(y_valid_split, y_proba),
            "avg_precision": safe_average_precision_score(y_valid_split, y_proba),
        }

        cm = confusion_matrix(y_valid_split, y_pred_opt, labels=[0, 1])
        tn, fp, fn, tp = cm.ravel()
        metrics_opt.update(
            {
                "true_negatives": tn,
                "false_positives": fp,
                "false_negatives": fn,
                "true_positives": tp,
                "sensitivity": tp / (tp + fn) if (tp + fn) > 0 else 0,
                "specificity": tn / (tn + fp) if (tn + fp) > 0 else 0,
            }
        )

        print(
            f"Fold {fold} – Acc: {metrics_opt['accuracy']:.3f}, "
            f"Precision: {metrics_opt['precision']:.3f}, Recall: {metrics_opt['recall']:.3f}, "
            f"F1: {metrics_opt['f1']:.3f}, AP: {metrics_opt['avg_precision']:.3f}"
        )

        fold_result = {
            "fold": fold,
            "optimal_threshold": opt_threshold,
            "train_size": len(X_train_split),
            "validation_size": len(X_valid_split),
            "test_size": len(X_test),
            "train_0_class": neg,
            "train_1_class": pos,
            "test_0_class": (y_test == 0).sum(),
            "test_1_class": (y_test == 1).sum(),
            "best_params": str(search.best_params_),
        }

        fold_result.update({f"optimal_{k}": v for k, v in metrics_opt.items()})
        fold_metrics.append(fold_result)
        feature_importances.append(clf.feature_importances_)

        if plot_folds and y_valid_split.nunique() > 1:
            plot_fold_results(
                y_valid_split,
                y_pred_opt,
                y_proba,
                clf,
                classification_features,
                opt_threshold,
                fold,
            )

    if not best_models:
        raise ValueError(
            "No valid classification folds were produced. Check class balance and data length."
        )

    # Select best model & average threshold
    best_model_info = max(best_models, key=lambda x: x["score"])
    best_model = best_model_info["model"]
    avg_opt_threshold = float(np.mean(optimal_thresholds))

    print_header(
        "🛸🛸🛸🛸 CLASSIFICATION – BEST HYPERPARAMETERS ACROSS FOLDS (FROM CV)"
    )
    for param, value in best_model_info["params"].items():
        print(f"🛸🛸 {param}: {value}")

    print_header("🍟🍟 CLASSIFICATION – FINAL 80/20 TEST ON LAST PART OF SERIES")
    if len(df_clean) < 2:
        raise ValueError(
            "Need at least 2 rows for final chronological train/test split."
        )

    final_n = max(1, int(len(df_clean) * test_size_final))
    final_n = min(final_n, len(df_clean) - 1)
    X_train_final = X.iloc[:-final_n]
    y_train_final = y.iloc[:-final_n]
    X_test_final = X.iloc[-final_n:]
    y_test_final = y.iloc[-final_n:]

    if y_train_final.nunique() < 2:
        raise ValueError("Final classification train split has only one class.")

    best_model.fit(X_train_final, y_train_final)
    y_proba_final = best_model.predict_proba(X_test_final)[:, 1]

    opt_threshold_final = choose_optimal_threshold(
        y_test_final, y_proba_final, default_threshold=avg_opt_threshold
    )

    y_pred_final = (y_proba_final >= opt_threshold_final).astype(int)

    metrics_final = {
        "accuracy": accuracy_score(y_test_final, y_pred_final),
        "precision": precision_score(y_test_final, y_pred_final, zero_division=0),
        "recall": recall_score(y_test_final, y_pred_final, zero_division=0),
        "f1": f1_score(y_test_final, y_pred_final, zero_division=0),
        "roc_auc": safe_roc_auc_score(y_test_final, y_proba_final),
        "avg_precision": safe_average_precision_score(y_test_final, y_proba_final),
    }

    cm_final = confusion_matrix(y_test_final, y_pred_final, labels=[0, 1])
    tn, fp, fn, tp = cm_final.ravel()
    metrics_final.update(
        {
            "true_negatives": tn,
            "false_positives": fp,
            "false_negatives": fn,
            "true_positives": tp,
            "sensitivity": tp / (tp + fn) if (tp + fn) > 0 else 0,
            "specificity": tn / (tn + fp) if (tn + fp) > 0 else 0,
        }
    )

    print(
        f"FINAL TEST – n={len(X_test_final)} | "
        f"Acc: {metrics_final['accuracy']:.3f}, "
        f"Prec: {metrics_final['precision']:.3f}, "
        f"Rec: {metrics_final['recall']:.3f}, "
        f"F1: {metrics_final['f1']:.3f}, AP: {metrics_final['avg_precision']:.3f}"
    )
    print(f"Final optimal threshold: {opt_threshold_final:.4f}")

    # Confusion matrix plot for final test
    plt.figure(figsize=(6, 5))

    total = cm_final.sum()
    # Simple: Count + Overall Percentage
    annot = []
    for i in range(cm_final.shape[0]):
        row = []
        for j in range(cm_final.shape[1]):
            count = cm_final[i, j]
            percentage = (count / total) * 100
            row.append(f"{count}\n({percentage:.1f}%)")
        annot.append(row)

    sns.heatmap(
        cm_final,
        annot=annot,
        fmt="",
        cmap="Blues",
        xticklabels=["No Curtailment", "Curtailment"],
        yticklabels=["No Curtailment", "Curtailment"],
    )
    plt.title("Confusion Matrix – Final Test Set")
    plt.xlabel("Predicted")
    plt.ylabel("Actual")
    plt.tight_layout()
    plt.show()

    # Save model & threshold info CLASSIFICATION
    model_metadata = None
    if save_model:
        metrics_df = pd.DataFrame(fold_metrics)
        model_metadata = {
            "model_type": "xgboost_classifier",
            "average_optimal_threshold": convert_to_serializable(avg_opt_threshold),
            "80/20_optimal_threshold": convert_to_serializable(opt_threshold_final),
            "feature_names": classification_features,
            "model_parameters": convert_to_serializable(best_model_info["params"]),
            "fold_metrics": convert_to_serializable(metrics_df.to_dict()),
            "all_optimal_thresholds": convert_to_serializable(optimal_thresholds),
            "final_test_metrics": convert_to_serializable(metrics_final),
            "inference_notes": {
                "target_column": "curtailment_flag",
                "decision_rule": "predict curtailment when predicted_probability >= 80/20_optimal_threshold",
                "required_features": classification_features,
            },
            "training_date": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M"),
            "category": category,
        }

        ensure_parent_dir(CLASS_MODEL_PATH)
        ensure_parent_dir(CLASS_METADATA_PATH)

        joblib.dump(best_model, CLASS_MODEL_PATH)
        with open(CLASS_METADATA_PATH, "w") as f:
            json.dump(convert_to_serializable(model_metadata), f, indent=4)

        print("\nClassification model & metadata saved.")
        print(CLASS_MODEL_PATH)
        print(CLASS_METADATA_PATH)

    # Summary
    metrics_df = pd.DataFrame(fold_metrics)
    avg_feature_importance = np.mean(feature_importances, axis=0)
    feature_importance_df = pd.DataFrame(
        {
            "feature": classification_features,
            "importance": avg_feature_importance,
        }
    ).sort_values("importance", ascending=False)

    plot_comprehensive_summary(metrics_df, feature_importance_df, X, y)

    return {
        "fold_metrics": metrics_df,
        "feature_importance": feature_importance_df,
        "avg_metrics": metrics_df[
            [c for c in metrics_df.columns if c.startswith("optimal_")]
        ].mean(),
        "classification_features": classification_features,
        "best_model": best_model,
        "average_optimal_threshold": avg_opt_threshold,
        "80/20_optimal_threshold": opt_threshold_final,
        "best_model_info": best_model_info,
        "model_metadata": model_metadata,
        "final_test_metrics": metrics_final,
    }


# =============================================================================
# REGRESSION – PLOTS & MAIN FUNCTION
# =============================================================================


def plot_regression_results(
    y_true,
    y_pred,
    model,
    feature_names,
    df_subset,
    time_col=None,
    title_suffix="",
):
    """Diagnostic plots for regression."""
    residuals = y_pred - y_true

    fig = plt.figure(figsize=(14, 10))
    gs = fig.add_gridspec(3, 2, height_ratios=[1.0, 1.0, 1.2], hspace=0.35, wspace=0.3)

    ax_scatter = fig.add_subplot(gs[0, 0])
    ax_resid = fig.add_subplot(gs[0, 1])
    ax_hist = fig.add_subplot(gs[1, 0])
    ax_import = fig.add_subplot(gs[1, 1])
    ax_time = fig.add_subplot(gs[2, :])

    # 1. Scatter
    ax_scatter.scatter(y_true, y_pred, alpha=0.4, s=10)
    max_val = max(y_true.max(), y_pred.max())
    min_val = min(y_true.min(), y_pred.min())
    ax_scatter.plot([min_val, max_val], [min_val, max_val], "k--", lw=1)
    ax_scatter.set_xlabel("Actual Curtailment (kWh/kW)")
    ax_scatter.set_ylabel("Predicted Curtailment (kWh/kW)")
    ax_scatter.set_title(f"Actual vs Predicted {title_suffix}")
    ax_scatter.xaxis.set_major_locator(MaxNLocator(6))
    ax_scatter.yaxis.set_major_locator(MaxNLocator(6))
    fmt = FuncFormatter(lambda x, pos: f"{x:.3f}")
    ax_scatter.xaxis.set_major_formatter(fmt)
    ax_scatter.yaxis.set_major_formatter(fmt)

    # 2. Residuals
    ax_resid.scatter(y_pred, residuals, alpha=0.4, s=10)
    ax_resid.axhline(0, color="black", linestyle="--", linewidth=1)
    ax_resid.set_xlabel("Predicted Curtailment (kWh/kW)")
    ax_resid.set_ylabel("Residuals (pred - true)")
    ax_resid.set_title("Residual Plot")
    ax_resid.xaxis.set_major_locator(MaxNLocator(6))
    ax_resid.yaxis.set_major_locator(MaxNLocator(5))
    ax_resid.xaxis.set_major_formatter(fmt)
    ax_resid.yaxis.set_major_formatter(fmt)
    ax_resid.grid(alpha=0.3)

    # 3. Residual histogram
    ax_hist.hist(residuals, bins=40, alpha=0.8, edgecolor="black")
    ax_hist.axvline(0, color="black", linestyle="--", linewidth=1)
    ax_hist.set_xlabel("Residuals (pred - true)")
    ax_hist.set_ylabel("Frequency")
    ax_hist.set_title("Residual Distribution")
    ax_hist.xaxis.set_major_locator(MaxNLocator(6))
    ax_hist.xaxis.set_major_formatter(fmt)
    ax_hist.grid(axis="y", alpha=0.3)

    # 4. Feature importance
    feature_importance = model.feature_importances_
    indices = np.argsort(feature_importance)[::-1]
    top_n = min(12, len(indices))
    indices = indices[:top_n]
    sorted_features = [feature_names[i] for i in indices]
    ax_import.barh(sorted_features[::-1], feature_importance[indices][::-1])
    ax_import.set_xlabel("Feature Importance")
    ax_import.set_title("Regression Feature Importance (Top Features)")
    ax_import.xaxis.set_major_locator(MaxNLocator(5))
    ax_import.grid(axis="x", alpha=0.3)

    # 5. Time plot
    if time_col is not None and time_col in df_subset.columns:
        t = df_subset[time_col]
        ax_time.plot(
            t,
            y_true.values,
            label="Actual",
            marker="o",
            linestyle="-",
            markersize=2,
            linewidth=0.8,
        )
        ax_time.plot(
            t,
            y_pred,
            label="Predicted",
            marker="x",
            linestyle="-",
            markersize=2,
            linewidth=0.8,
        )
        ax_time.set_xlabel("Time")
    else:
        idx = np.arange(len(y_true))
        ax_time.plot(
            idx,
            y_true.values,
            label="Actual",
            marker="o",
            linestyle="-",
            markersize=2,
            linewidth=0.8,
        )
        ax_time.plot(
            idx,
            y_pred,
            label="Predicted",
            marker="x",
            linestyle="-",
            markersize=2,
            linewidth=0.8,
        )
        ax_time.set_xlabel("Sample Index")

    ax_time.set_ylabel("Curtailment (kWh/kW)")
    ax_time.set_title("Actual vs Predicted Over Time")
    ax_time.legend(loc="upper right", fontsize=9)
    ax_time.yaxis.set_major_locator(MaxNLocator(6))
    ax_time.yaxis.set_major_formatter(fmt)
    ax_time.grid(alpha=0.3)
    plt.setp(ax_time.get_xticklabels(), rotation=45, ha="right")

    plt.tight_layout()
    plt.show()


def robust_curtailment_regression_with_search(
    df,
    time_col_candidates=("delivery_start_berlin", "time_berlin", "timestamp"),
    test_size: float = 0.3,
    n_splits_cv: int = 3,
    n_iter_search: int = 50,
    save_model: bool = True,
):
    """Regression on curtailment_kWh_per_kw for curtailment_flag == 1."""
    print_header("CURTAILMENT REGRESSION ANALYSIS")

    df = df.copy()
    validate_required_columns(df, ["curtailment_kWh_per_kw"], "Regression input")

    if "volume__mw_imbalance" not in df.columns:
        df["volume__mw_imbalance"] = 0

    # Time sorting
    time_col = None
    for c in time_col_candidates:
        if c in df.columns:
            time_col = c
            df = df.sort_values(by=time_col)
            break
    if time_col is None:
        print("⚠️ No explicit time column found, using index order as time.")
        df = df.sort_index()

    # Target & basic features
    df["curtailment_flag"] = (df["curtailment_kWh_per_kw"] > 0).astype(int)
    df["volume__mw_imbalance"] = pd.to_numeric(
        df["volume__mw_imbalance"], errors="coerce"
    ).fillna(0)

    candidate_reg_features = [
        "quarterly_energy_kWh_per_kw",
        "enwex_percentage",
        "dayaheadprice_eur_mwh",
        "rebap_euro_per_mwh",
        "volume__mw_imbalance",
        "id500_eur_mwh",
        "rmv_eur_per_mwh",
    ]
    for col in candidate_reg_features:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").ffill().bfill()

    # regression features
    reg_features = [f for f in candidate_reg_features if f in df.columns]
    if not reg_features:
        raise ValueError("No valid regression features found in dataframe.")

    df = df[df["curtailment_flag"] == 1].copy()
    df = df.dropna(subset=reg_features + ["curtailment_kWh_per_kw"])

    # df = df.dropna(subset=reg_features)

    if df.empty:
        raise ValueError(
            "No rows left for regression after filtering curtailment_flag == 1 and NaN drop."
        )

    X = df[reg_features]
    y = df["curtailment_kWh_per_kw"]

    print(f"Total usable rows for regression: {len(df)}")

    base_model = XGBRegressor(
        objective="reg:squarederror",
        random_state=42,
    )

    # regression parameters
    param_distributions = {
        "n_estimators": randint(300, 4000),
        "learning_rate": uniform(0.005, 0.8),
        "max_depth": randint(2, 10),
        "subsample": uniform(0.4, 0.6),
        "colsample_bytree": uniform(0.4, 0.6),
        "reg_alpha": uniform(0.0, 1.0),
        "reg_lambda": uniform(0.0, 1.0),
        "gamma": uniform(0.0, 0.7),
    }

    print("Running fold-wise RandomizedSearchCV for regression...")

    outer_splits = get_safe_n_splits(len(X), n_splits_cv)
    tscv = TimeSeriesSplit(n_splits=outer_splits)

    fold_metrics = []
    best_models = []
    for fold, (train_idx, test_idx) in enumerate(tscv.split(X), 1):
        print(f"FOLD {fold} – Train size: {len(train_idx)}, Test size: {len(test_idx)}")

        X_train, _ = X.iloc[train_idx], X.iloc[test_idx]
        y_train, _ = y.iloc[train_idx], y.iloc[test_idx]

        if len(X_train) < 5:
            print(f"Skipping fold {fold}: not enough training rows ({len(X_train)}).")
            continue

        split_idx = int(len(X_train) * 0.8)
        split_idx = max(1, min(split_idx, len(X_train) - 1))

        X_train_split, X_valid_split = X_train[:split_idx], X_train[split_idx:]
        y_train_split, y_valid_split = y_train[:split_idx], y_train[split_idx:]

        inner_splits = get_safe_n_splits(len(X_train_split), min(3, n_splits_cv))
        search = RandomizedSearchCV(
            estimator=base_model,
            param_distributions=param_distributions,
            n_iter=n_iter_search,
            cv=TimeSeriesSplit(n_splits=inner_splits),
            scoring="neg_mean_absolute_error",
            n_jobs=-1,
            verbose=0,
            random_state=42,
            refit=True,
        )

        search.fit(X_train_split, y_train_split)
        fold_best_model = search.best_estimator_

        y_valid_pred = fold_best_model.predict(X_valid_split)
        valid_mae = mean_absolute_error(y_valid_split, y_valid_pred)
        print(f"Fold {fold} – Validation MAE: {valid_mae:.4f}")

        best_models.append(
            {
                "model": fold_best_model,
                "score": search.best_score_,
                "params": search.best_params_,
                "fold": fold,
                "validation_mae": valid_mae,
            }
        )

        fold_metrics.append(
            {
                "fold": fold,
                "best_params": search.best_params_,
                "validation_mae": valid_mae,
            }
        )

    if not best_models:
        raise ValueError(
            "No valid regression folds were produced. Check data length and missing values."
        )

    best_model_info = min(best_models, key=lambda x: x["validation_mae"])
    best_model_params = best_model_info["params"]

    print(f"Best Model based on MAE across folds: {best_model_params}")
    print(f"Best fold Validation MAE: {best_model_info['validation_mae']:.4f}")

    split_idx = int(len(X) * (1 - test_size))
    split_idx = max(1, min(split_idx, len(X) - 1))
    X_train1, X_test1 = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train1, y_test1 = y.iloc[:split_idx], y.iloc[split_idx:]
    df_test1 = df.iloc[split_idx:]

    final_model = XGBRegressor(
        objective="reg:squarederror",
        random_state=42,
        **best_model_params,
    )
    final_model.fit(X_train1, y_train1)

    print(f"Train size: {len(X_train1)}, Test size: {len(X_test1)}")
    y_pred_test1 = final_model.predict(X_test1)

    mse_test = mean_squared_error(y_test1, y_pred_test1)
    mae_test = mean_absolute_error(y_test1, y_pred_test1)
    mape_test = mean_absolute_percentage_error(y_test1, y_pred_test1)
    r2_test = r2_score(y_test1, y_pred_test1)

    print(
        f"TEST – MSE: {mse_test:.4f}, MAE: {mae_test:.4f}, "
        f"MAPE: {mape_test:.4f}, R²: {r2_test:.4f}"
    )

    reg_metadata = {
        "model_type": "xgboost_regressor",
        "feature_names": reg_features,
        "model_parameters": convert_to_serializable(best_model_params),
        "fold_metrics": convert_to_serializable(fold_metrics),
        "final_test_metrics": {
            "mse": convert_to_serializable(mse_test),
            "mae": convert_to_serializable(mae_test),
            "mape": convert_to_serializable(mape_test),
            "r2": convert_to_serializable(r2_test),
        },
        "inference_notes": {
            "target_column": "curtailment_kWh_per_kw",
            "pre_filter": "curtailment_flag == 1",
            "required_features": reg_features,
        },
        "training_date": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M"),
        "category": category,
    }

    if save_model:
        ensure_parent_dir(REG_MODEL_PATH)
        ensure_parent_dir(REG_METADATA_PATH)

        joblib.dump(final_model, REG_MODEL_PATH)
        with open(REG_METADATA_PATH, "w") as f:
            json.dump(convert_to_serializable(reg_metadata), f, indent=4)

        print("Regression model & metadata saved.")
        print(REG_MODEL_PATH)
        print(REG_METADATA_PATH)

    plot_regression_results(
        y_true=y_test1,
        y_pred=y_pred_test1,
        model=final_model,
        feature_names=reg_features,
        df_subset=df_test1,
        time_col=time_col,
        title_suffix="(Regression 70/30 Test Split)",
    )

    return {
        "best_model": final_model,
        "best_params": best_model_params,
        "cv_best_score_neg_mae": best_model_info["score"],
        "fold_metrics": fold_metrics,
        "test_metrics": {
            "mse": mse_test,
            "mae": mae_test,
            "mape": mape_test,
            "r2": r2_test,
        },
        "train_size": len(X_train1),
        "test_size": len(X_test1),
        "features": reg_features,
        "time_col": time_col,
        "metadata": reg_metadata,
    }


# =============================================================================
# PATH CONFIG
# =============================================================================


def set_paths_for_category(category):
    if category == "PV_rules":
        return {
            "DATA_PATH": "/data/flexpwr_datalore_origination_prod_bucket/curtailment_forecast_upgrid/PV_rules/forecast_data_PV_rules.parquet",
            "CLASS_MODEL_PATH": "/data/flexpwr_datalore_origination_prod_bucket/curtailment_forecast_upgrid/PV_rules/model result_v2/classification_best_model_PV_rules.joblib",
            "CLASS_METADATA_PATH": "/data/flexpwr_datalore_origination_prod_bucket/curtailment_forecast_upgrid/PV_rules/model result_v2/classification_metadata_PV_rules.json",
            "REG_MODEL_PATH": "/data/flexpwr_datalore_origination_prod_bucket/curtailment_forecast_upgrid/PV_rules/model result_v2/regression_best_model_PV_rules.joblib",
            "REG_METADATA_PATH": "/data/flexpwr_datalore_origination_prod_bucket/curtailment_forecast_upgrid/PV_rules/model result_v2/regression_metadata_PV_rules.json",
        }

    elif category == "PV_no_rules":
        return {
            "DATA_PATH": "/data/flexpwr_datalore_origination_prod_bucket/curtailment_forecast_upgrid/PV_no_rules/forecast_data_PV_no_rules.parquet",
            "CLASS_MODEL_PATH": "/data/flexpwr_datalore_origination_prod_bucket/curtailment_forecast_upgrid/PV_no_rules/model result_v2/classification_best_model_PV_no_rules.joblib",
            "CLASS_METADATA_PATH": "/data/flexpwr_datalore_origination_prod_bucket/curtailment_forecast_upgrid/PV_no_rules/model result_v2/classification_metadata_PV_no_rules.json",
            "REG_MODEL_PATH": "/data/flexpwr_datalore_origination_prod_bucket/curtailment_forecast_upgrid/PV_no_rules/model result_v2/regression_best_model_PV_no_rules.joblib",
            "REG_METADATA_PATH": "/data/flexpwr_datalore_origination_prod_bucket/curtailment_forecast_upgrid/PV_no_rules/model result_v2/regression_metadata_PV_no_rules.json",
        }

    elif category == "WIND_rules":
        return {
            "DATA_PATH": "/data/flexpwr_datalore_origination_prod_bucket/curtailment_forecast_upgrid/WIND_rules/forecast_data_WIND_rules.parquet",
            "CLASS_MODEL_PATH": "/data/flexpwr_datalore_origination_prod_bucket/curtailment_forecast_upgrid/WIND_rules/model result_v2/classification_best_model_WIND_rules.joblib",
            "CLASS_METADATA_PATH": "/data/flexpwr_datalore_origination_prod_bucket/curtailment_forecast_upgrid/WIND_rules/model result_v2/classification_metadata_WIND_rules.json",
            "REG_MODEL_PATH": "/data/flexpwr_datalore_origination_prod_bucket/curtailment_forecast_upgrid/WIND_rules/model result_v2/regression_best_model_WIND_rules.joblib",
            "REG_METADATA_PATH": "/data/flexpwr_datalore_origination_prod_bucket/curtailment_forecast_upgrid/WIND_rules/model result_v2/regression_metadata_WIND_rules.json",
        }

    elif category == "WIND_no_rules":
        return {
            "DATA_PATH": "/data/flexpwr_datalore_origination_prod_bucket/curtailment_forecast_upgrid/WIND_no_rules/forecast_data_WIND_no_rules.parquet",
            "CLASS_MODEL_PATH": "/data/flexpwr_datalore_origination_prod_bucket/curtailment_forecast_upgrid/WIND_no_rules/model result_v2/classification_best_model_WIND_no_rules.joblib",
            "CLASS_METADATA_PATH": "/data/flexpwr_datalore_origination_prod_bucket/curtailment_forecast_upgrid/WIND_no_rules/model result_v2/classification_metadata_WIND_no_rules.json",
            "REG_MODEL_PATH": "/data/flexpwr_datalore_origination_prod_bucket/curtailment_forecast_upgrid/WIND_no_rules/model result_v2/regression_best_model_WIND_no_rules.joblib",
            "REG_METADATA_PATH": "/data/flexpwr_datalore_origination_prod_bucket/curtailment_forecast_upgrid/WIND_no_rules/model result_v2/regression_metadata_WIND_no_rules.json",
        }

    else:
        raise ValueError(f"Unknown category: {category}")


category = "PV_no_rules"
paths = set_paths_for_category(category)

DATA_PATH = paths["DATA_PATH"]
CLASS_MODEL_PATH = paths["CLASS_MODEL_PATH"]
CLASS_METADATA_PATH = paths["CLASS_METADATA_PATH"]
REG_MODEL_PATH = paths["REG_MODEL_PATH"]
REG_METADATA_PATH = paths["REG_METADATA_PATH"]


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    print_header("LOADING DATA")
    df_all = pd.read_parquet(DATA_PATH)
    print(f"Category: {category}")
    print(f"Loaded dataframe with {len(df_all):,} rows")

    TEST_SIZE = 0.25
    N_SPLITS = 3

    # 1) Classification
    cls_results = comprehensive_classification_analysis(
        df_all,
        test_size_final=TEST_SIZE,
        n_splits=N_SPLITS,
        n_iter=50,
        save_model=True,
        plot_folds=True,
    )

    # 2) Regression
    reg_results = robust_curtailment_regression_with_search(
        df_all,
        test_size=TEST_SIZE,
        n_splits_cv=N_SPLITS,
        n_iter_search=300,
        save_model=True,
    )

    print_header("SUMMARY – CLASSIFICATION AVG METRICS")
    print(cls_results["avg_metrics"])

    print_header("🍟🍟 SUMMARY – REGRESSION TEST METRICS")
    print(reg_results["test_metrics"])
