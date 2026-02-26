import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import joblib
import json
import warnings

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
    mean_squared_error,
    mean_absolute_error,
    mean_absolute_percentage_error,
    r2_score,
)

warnings.filterwarnings("ignore")


# =============================================================================
# PATH CONFIG
# =============================================================================


def set_paths_for_category(category):
    if category == "PV_rules":
        return {
            "CLASS_MODEL_PATH": "/data/flexpwr_datalore_origination_prod_bucket/curtailment_forecast_upgrid/PV_rules/model result_v2/classification_best_model_PV_rules.joblib",
            "CLASS_METADATA_PATH": "/data/flexpwr_datalore_origination_prod_bucket/curtailment_forecast_upgrid/PV_rules/model result_v2/classification_metadata_PV_rules.json",
            "REG_MODEL_PATH": "/data/flexpwr_datalore_origination_prod_bucket/curtailment_forecast_upgrid/PV_rules/model result_v2/regression_best_model_PV_rules.joblib",
            "REG_METADATA_PATH": "/data/flexpwr_datalore_origination_prod_bucket/curtailment_forecast_upgrid/PV_rules/model result_v2/regression_metadata_PV_rules.json",
        }

    elif category == "PV_no_rules":
        return {
            "CLASS_MODEL_PATH": "/data/flexpwr_datalore_origination_prod_bucket/curtailment_forecast_upgrid/PV_no_rules/model result_v2/classification_best_model_PV_no_rules.joblib",
            "CLASS_METADATA_PATH": "/data/flexpwr_datalore_origination_prod_bucket/curtailment_forecast_upgrid/PV_no_rules/model result_v2/classification_metadata_PV_no_rules.json",
            "REG_MODEL_PATH": "/data/flexpwr_datalore_origination_prod_bucket/curtailment_forecast_upgrid/PV_no_rules/model result_v2/regression_best_model_PV_no_rules.joblib",
            "REG_METADATA_PATH": "/data/flexpwr_datalore_origination_prod_bucket/curtailment_forecast_upgrid/PV_no_rules/model result_v2/regression_metadata_PV_no_rules.json",
        }

    elif category == "WIND_rules":
        return {
            "CLASS_MODEL_PATH": "/data/flexpwr_datalore_origination_prod_bucket/curtailment_forecast_upgrid/WIND_rules/model result_v2/classification_best_model_WIND_rules.joblib",
            "CLASS_METADATA_PATH": "/data/flexpwr_datalore_origination_prod_bucket/curtailment_forecast_upgrid/WIND_rules/model result_v2/classification_metadata_WIND_rules.json",
            "REG_MODEL_PATH": "/data/flexpwr_datalore_origination_prod_bucket/curtailment_forecast_upgrid/WIND_rules/model result_v2/regression_best_model_WIND_rules.joblib",
            "REG_METADATA_PATH": "/data/flexpwr_datalore_origination_prod_bucket/curtailment_forecast_upgrid/WIND_rules/model result_v2/regression_metadata_WIND_rules.json",
        }

    elif category == "WIND_no_rules":
        return {
            "CLASS_MODEL_PATH": "/data/flexpwr_datalore_origination_prod_bucket/curtailment_forecast_upgrid/WIND_no_rules/model result_v2/classification_best_model_WIND_no_rules.joblib",
            "CLASS_METADATA_PATH": "/data/flexpwr_datalore_origination_prod_bucket/curtailment_forecast_upgrid/WIND_no_rules/model result_v2/classification_metadata_WIND_no_rules.json",
            "REG_MODEL_PATH": "/data/flexpwr_datalore_origination_prod_bucket/curtailment_forecast_upgrid/WIND_no_rules/model result_v2/regression_best_model_WIND_no_rules.joblib",
            "REG_METADATA_PATH": "/data/flexpwr_datalore_origination_prod_bucket/curtailment_forecast_upgrid/WIND_no_rules/model result_v2/regression_metadata_WIND_no_rules.json",
        }

    else:
        raise ValueError(f"Unknown category: {category}")


category = "PV_no_rules"
paths = set_paths_for_category(category)

CLASS_MODEL_PATH = paths["CLASS_MODEL_PATH"]
CLASS_METADATA_PATH = paths["CLASS_METADATA_PATH"]
REG_MODEL_PATH = paths["REG_MODEL_PATH"]
REG_METADATA_PATH = paths["REG_METADATA_PATH"]


# =============================================================================
# SMALL HELPERS
# =============================================================================


def print_header(title: str):
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


# =============================================================================
# CLASSIFICATION IMPLEMENTATION
# =============================================================================


def feature_engineering_classification(df: pd.DataFrame, feature_names):
    """Feature engineering for classification – must match training."""
    df = df.copy()

    # volume__mw_imbalance
    if "volume__mw_imbalance" in df.columns:
        df["volume__mw_imbalance"] = pd.to_numeric(
            df["volume__mw_imbalance"], errors="coerce"
        ).fillna(0)
    else:
        df["volume__mw_imbalance"] = 0.0

    # Target flag if actuals present (for metrics)
    if "curtailment_kWh_per_kw" in df.columns:
        df["curtailment_flag"] = (df["curtailment_kWh_per_kw"] > 0).astype(int)
        has_actual_values = True
    else:
        has_actual_values = False

    # price-related flags
    for col in ["dayaheadprice_eur_mwh", "rebap_euro_per_mwh"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["DA_negative_flag"] = (df["dayaheadprice_eur_mwh"] < 0).astype(int)
    df["DA_negative_flag_lag_1"] = df["DA_negative_flag"].shift(1)

    df["rebap_negative_flag"] = (df["rebap_euro_per_mwh"] < 0).astype(int)
    df["rebap_negative_flag_lag_1"] = df["rebap_negative_flag"].shift(1)

    # which features are available
    available_features = [f for f in feature_names if f in df.columns]
    missing_features = [f for f in feature_names if f not in df.columns]

    if missing_features:
        print(f"⚠️ Missing classification features: {missing_features}")
        if not available_features:
            raise ValueError("No required classification features available.")

    # drop rows with missing in features
    df_clean = df.dropna(subset=available_features).copy()
    if df_clean.empty:
        raise ValueError(
            "No valid rows after classification cleaning (NaNs in features)."
        )

    # ensure numeric
    for f in available_features:
        df_clean[f] = pd.to_numeric(df_clean[f], errors="coerce")

    return df_clean, available_features, has_actual_values


def predict_curtailment_classification(
    df_new_prediction: pd.DataFrame,
    model_path: str = CLASS_MODEL_PATH,
    metadata_path: str = CLASS_METADATA_PATH,
    plot: bool = False,
):
    """Run classification model on new data."""
    print_header("CLASSIFICATION – LOADING MODEL & METADATA")
    try:
        best_model = joblib.load(model_path)
        with open(metadata_path, "r") as f:
            model_metadata = json.load(f)
    except FileNotFoundError as e:
        print(f"❌ Error loading classification files: {e}")
        return None

    feature_names = model_metadata["feature_names"]
    average_optimal_threshold = model_metadata["average_optimal_threshold"]

    print(f"Using optimal threshold: {average_optimal_threshold:.4f}")

    print_header("CLASSIFICATION – FEATURE ENGINEERING")
    df_clean, available_features, has_actual_values = (
        feature_engineering_classification(df_new_prediction, feature_names)
    )

    X_new = df_clean[available_features]
    print(f"Classification rows: {len(X_new)}, features used: {available_features}")

    print_header("CLASSIFICATION – PREDICTION")
    y_proba = best_model.predict_proba(X_new)[:, 1]
    y_pred = (y_proba >= average_optimal_threshold).astype(int)

    df_clean["predicted_curtailment_probability"] = y_proba
    df_clean["predicted_curtailment_flag"] = y_pred
    df_clean["prediction_timestamp_cls"] = pd.Timestamp.now()

    print(
        f"Predicted curtailment == 1 for {y_pred.sum():,} rows "
        f"({y_pred.mean()*100:.1f}% of classified rows)."
    )

    # metrics if actuals
    if has_actual_values and "curtailment_flag" in df_clean.columns:
        y_actual = df_clean["curtailment_flag"]
        accuracy = accuracy_score(y_actual, y_pred)
        precision = precision_score(y_actual, y_pred, zero_division=0)
        recall = recall_score(y_actual, y_pred, zero_division=0)
        f1 = f1_score(y_actual, y_pred, zero_division=0)
        roc_auc = roc_auc_score(y_actual, y_proba)
        avg_precision = average_precision_score(y_actual, y_proba)

        print_header("CLASSIFICATION – METRICS (ACTUALS AVAILABLE)")
        print(f"Accuracy:      {accuracy:.4f}")
        print(f"Precision:     {precision:.4f}")
        print(f"Recall:        {recall:.4f}")
        print(f"F1-Score:      {f1:.4f}")
        print(f"ROC AUC:       {roc_auc:.4f}")
        print(f"Avg Precision: {avg_precision:.4f}")
    else:
        accuracy = precision = recall = f1 = roc_auc = avg_precision = None
        print("ℹ️ No actual curtailment available for classification metrics.")

    # simple visualization if requested
    if plot:
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4))

        # Linear scale
        sns.histplot(y_proba, bins=30, kde=True, ax=ax1)
        ax1.axvline(
            average_optimal_threshold, color="red", linestyle="--", label="Threshold"
        )
        ax1.set_title("Linear Scale")
        ax1.set_xlabel("P(curtailment=1)")
        ax1.set_ylabel("Frequency")
        ax1.legend()

        # Log scale
        sns.histplot(y_proba, bins=30, kde=True, ax=ax2)
        ax2.axvline(
            average_optimal_threshold, color="red", linestyle="--", label="Threshold"
        )
        ax2.set_yscale("log")
        ax2.set_title("Log Scale")
        ax2.set_xlabel("P(curtailment=1)")
        ax2.set_ylabel("Frequency (log scale)")
        ax2.legend()

        plt.suptitle("Predicted Probability Distribution", fontsize=14)
        plt.tight_layout()
        plt.show()

        # y_actual_1 = df_clean["curtailment_flag"]
        # cm_final = confusion_matrix(y_actual_1, y_pred)
        # plt.figure(figsize=(6, 5))
        # sns.heatmap(
        #     cm_final,
        #     annot=True,
        #     fmt='d',
        #     cmap='Blues',
        #     xticklabels=['No Curtailment', 'Curtailment'],
        #     yticklabels=['No Curtailment', 'Curtailment'],
        # )
        # plt.title('Confusion Matrix – testing new data')
        # plt.xlabel('Predicted')
        # plt.ylabel('Actual')
        # plt.tight_layout()
        # plt.show()

        y_actual_1 = df_clean["curtailment_flag"]
        cm_final = confusion_matrix(y_actual_1, y_pred)
        total = cm_final.sum()

        plt.figure(figsize=(6, 5))

        # Simple: Count + Overall Percentage
        annot = []
        for i in range(cm_final.shape[0]):
            row = []
            for j in range(cm_final.shape[1]):
                count = cm_final[i, j]
                percentage = (count / total) * 100
                row.append(f"{count}\n({percentage:.1f}%)")
            annot.append(row)

        ax = sns.heatmap(
            cm_final,
            annot=annot,
            fmt="",
            cmap="Blues",
            xticklabels=["No Curtailment", "Curtailment"],
            yticklabels=["No Curtailment", "Curtailment"],
        )

        # Add total count
        plt.title("Confusion Matrix – testing new data")
        plt.xlabel("Predicted")
        plt.ylabel("Actual")
        plt.tight_layout()
        plt.show()

    results = {
        "predictions": df_clean,
        "model": best_model,
        "features_used": available_features,
        "optimal_threshold": average_optimal_threshold,
        "prediction_metrics": {
            "accuracy": accuracy,
            "precision": precision,
            "recall": recall,
            "f1": f1,
            "roc_auc": roc_auc,
            "avg_precision": avg_precision,
        },
        "prediction_stats": {
            "positive_predictions": int(y_pred.sum()),
            "negative_predictions": int(len(y_pred) - y_pred.sum()),
            "positive_rate": float(y_pred.mean()),
            "mean_probability": float(y_proba.mean()),
            "std_probability": float(y_proba.std()),
            "total_predictions": int(len(y_pred)),
        },
    }
    return results


# =============================================================================
# REGRESSION IMPLEMENTATION
# =============================================================================


def feature_engineering_regression(df: pd.DataFrame, reg_features):
    """Feature engineering for regression – must match training."""
    df = df.copy()

    # Define exogenous features (as at training)
    exo_features = [
        "quarterly_energy_kWh_per_kw",
        "enwex_percentage",
        "dayaheadprice_eur_mwh",
        "rebap_euro_per_mwh",
        "volume__mw_imbalance",
        "id500_eur_mwh",
        "rmv_eur_per_mwh",
    ]

    for col in exo_features:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").ffill().bfill()
        else:
            print(f"⚠️ Regression: missing feature {col} – filled with 0.")
            df[col] = 0.0

    # Lag features (not used in reg_features currently, but safe to create)
    if "curtailment_kWh_per_kw" in df.columns:
        df["curt_lag_1"] = df["curtailment_kWh_per_kw"].shift(1)
        df["curt_lag_2"] = df["curtailment_kWh_per_kw"].shift(2)

    available_features = [f for f in reg_features if f in df.columns]
    missing_features = [f for f in reg_features if f not in df.columns]

    if missing_features:
        print(f"⚠️ Missing regression features: {missing_features}")
        if not available_features:
            raise ValueError("No required regression features available.")

    df_clean = df.dropna(subset=available_features).copy()
    if df_clean.empty:
        raise ValueError("No valid rows after regression cleaning (NaNs in features).")

    X_new = df_clean[available_features].apply(pd.to_numeric, errors="coerce")

    return df_clean, X_new, available_features


# def plot_regression_predictions(df_clean, y_pred):
#     """Simple regression prediction distribution plot."""
#     fig, axes = plt.subplots(1, 2, figsize=(12, 5))

#     axes[0].hist(y_pred, bins=30, alpha=0.7, color='green', edgecolor="black")
#     axes[0].set_xlabel("Predicted Curtailment (kWh/kW)")
#     axes[0].set_ylabel("Frequency")
#     axes[0].set_title("Prediction Distribution")
#     axes[0].grid(True, alpha=0.3)

#     time_col = None
#     for candidate in ["delivery_start_berlin", "time_berlin", "timestamp"]:
#         if candidate in df_clean.columns:
#             time_col = candidate
#             break

#     if time_col:
#         df_sorted = df_clean.sort_values(time_col)
#         axes[1].plot(df_sorted[time_col], df_sorted["predicted_curtailment_kWh_per_kw"],
#                      color='red', linewidth=1.2)
#         axes[1].set_xlabel("Time")
#         axes[1].set_ylabel("Predicted Curtailment (kWh/kW)")
#         axes[1].set_title("Predictions Over Time")
#         axes[1].tick_params(axis='x', rotation=45)
#     else:
#         axes[1].plot(range(len(y_pred)), y_pred, color='red', linewidth=1.2)
#         axes[1].set_xlabel("Sample Index")
#         axes[1].set_ylabel("Predicted Curtailment (kWh/kW)")
#         axes[1].set_title("Predictions by Index")

#     plt.tight_layout()
#     plt.show()


def plot_regression_predictions(df_clean):
    """
    Simple regression prediction distribution + time plot,
    including actual curtailment if available.

    Expects:
        - df_clean["predicted_curtailment_kWh_per_kw"]
        - optionally df_clean["curtailment_kWh_per_kw"]
    """
    # Predicted values
    y_pred = df_clean["predicted_curtailment_kWh_per_kw"].values

    # Actual values (if available)
    has_actual = "curtailment_kWh_per_kw" in df_clean.columns
    y_actual = df_clean["curtailment_kWh_per_kw"].values if has_actual else None

    y_actual_plot = df_clean[df_clean["curtailment_kWh_per_kw"] > 0][
        "curtailment_kWh_per_kw"
    ].values

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))

    # -------------------------------------------------------------------------
    # 1) Distribution panel
    # -------------------------------------------------------------------------
    if has_actual:
        axes[0].hist(
            y_actual_plot,
            bins=30,
            alpha=0.6,
            color="blue",
            edgecolor="black",
            label="Actual",
        )

        axes[0].hist(
            y_pred,
            bins=30,
            alpha=0.6 if has_actual else 0.7,
            color="green",
            edgecolor="black",
            label="Predicted",
        )

        axes[0].set_xlabel("Curtailment (kWh/kW)")
        axes[0].set_ylabel("Frequency")
        axes[0].set_title("Curtailment Distribution")
        axes[0].grid(True, alpha=0.3)

        if has_actual:
            axes[0].legend()

    # -------------------------------------------------------------------------
    # 2) Time / index panel
    # -------------------------------------------------------------------------
    time_col = None
    for candidate in ["delivery_start_berlin", "time_berlin", "timestamp"]:
        if candidate in df_clean.columns:
            time_col = candidate
            break

    if time_col:
        df_sorted = df_clean.sort_values(time_col)
        x_vals = df_sorted[time_col]
        y_pred_sorted = df_sorted["predicted_curtailment_kWh_per_kw"]
        axes[1].plot(
            x_vals,
            y_pred_sorted,
            color="red",
            linewidth=1,
            label="Predicted",
        )
        if has_actual:
            axes[1].plot(
                x_vals,
                df_sorted["curtailment_kWh_per_kw"],
                color="blue",
                linewidth=0.8,
                alpha=0.4,
                label="Actual",
            )
        axes[1].set_xlabel("Time")
        axes[1].tick_params(axis="x", rotation=45)
    else:
        x_vals = np.arange(len(y_pred))
        axes[1].plot(
            x_vals,
            y_pred,
            color="red",
            linewidth=1.2,
            label="Predicted",
        )
        if has_actual:
            axes[1].plot(
                x_vals,
                y_actual,
                color="blue",
                linewidth=1.2,
                alpha=0.8,
                label="Actual",
            )
        axes[1].set_xlabel("Sample Index")

    axes[1].set_ylabel("Curtailment (kWh/kW)")
    axes[1].set_title("Curtailment Over Time")
    if has_actual:
        axes[1].legend()
    axes[1].grid(True, alpha=0.3)

    plt.tight_layout()
    plt.show()


def predict_curtailment_regression(
    df_reg_input: pd.DataFrame,
    model_path: str = REG_MODEL_PATH,
    metadata_path: str = REG_METADATA_PATH,
    plot: bool = False,
):
    """
    Run regression model on subset of rows (already filtered by classification).
    """
    print_header("REGRESSION – LOADING MODEL & METADATA")
    try:
        best_model = joblib.load(model_path)
        with open(metadata_path, "r") as f:
            reg_metadata = json.load(f)
    except FileNotFoundError as e:
        print(f"❌ Error loading regression files: {e}")
        return None

    # regression features read from metadata (matches training exactly)
    reg_features = reg_metadata["feature_names"]

    if df_reg_input.empty:
        print("ℹ️ No rows passed to regression (no predicted curtailment = 1).")
        return {
            "predictions": df_reg_input.assign(predicted_curtailment_kWh_per_kw=np.nan),
            "model": best_model,
            "features_used": reg_features,
            "prediction_metrics": {"mse": None, "mae": None, "mape": None, "r2": None},
            "prediction_stats": {
                "mean": None,
                "std": None,
                "min": None,
                "max": None,
                "count": 0,
            },
        }

    print_header("REGRESSION – FEATURE ENGINEERING ON FILTERED ROWS")
    df_clean, X_new, used_features = feature_engineering_regression(
        df_reg_input, reg_features
    )

    print(f"Regression rows: {len(X_new)}, features used: {used_features}")

    print_header("REGRESSION – PREDICTION")
    y_pred = best_model.predict(X_new)
    df_clean["predicted_curtailment_kWh_per_kw"] = y_pred
    df_clean["prediction_timestamp_reg"] = pd.Timestamp.now()

    # metrics if actual curtailment present
    if "curtailment_kWh_per_kw" in df_clean.columns:
        y_actual = df_clean["curtailment_kWh_per_kw"]
        mse = mean_squared_error(y_actual, y_pred)
        mae = mean_absolute_error(y_actual, y_pred)
        mape = mean_absolute_percentage_error(y_actual, y_pred)
        r2 = r2_score(y_actual, y_pred)

        print_header("REGRESSION – METRICS (ACTUALS AVAILABLE)")
        print(f"MSE:  {mse:.4f}")
        print(f"MAE:  {mae:.4f}")
        print(f"MAPE: {mape:.4f}")
        print(f"R²:   {r2:.4f}")
    else:
        mse = mae = mape = r2 = None
        print("ℹ️ No actual curtailment available for regression metrics.")

    if plot:
        # plot_regression_predictions(df_clean, y_pred)
        plot_regression_predictions(df_clean)

    results = {
        "predictions": df_clean,
        "model": best_model,
        "features_used": used_features,
        "prediction_metrics": {
            "mse": mse,
            "mae": mae,
            "mape": mape,
            "r2": r2,
        },
        "prediction_stats": {
            "mean": float(np.mean(y_pred)),
            "std": float(np.std(y_pred)),
            "min": float(np.min(y_pred)),
            "max": float(np.max(y_pred)),
            "count": int(len(y_pred)),
        },
    }
    return results


# =============================================================================
# FULL PIPELINE: CLASSIFICATION -> REGRESSION (FILTERED)
# =============================================================================


def run_curtailment_forecast(
    df_new_prediction: pd.DataFrame,
    cls_model_path: str = CLASS_MODEL_PATH,
    cls_metadata_path: str = CLASS_METADATA_PATH,
    reg_model_path: str = REG_MODEL_PATH,
    reg_metadata_path: str = REG_METADATA_PATH,
    plot_class: bool = False,
    plot_reg: bool = False,
):
    """
    Full pipeline:
      1) Classification on all rows
      2) Filter rows with predicted_curtailment_flag == 1
      3) Regression only on that filtered subset
      4) Merge regression predictions back into main dataframe
    """
    # 1. Classification
    cls_results = predict_curtailment_classification(
        df_new_prediction,
        model_path=cls_model_path,
        metadata_path=cls_metadata_path,
        plot=plot_class,
    )
    if cls_results is None:
        return None

    df_cls = cls_results["predictions"].copy()

    # 2. Filter rows with predicted_curtailment_flag == 1
    if "predicted_curtailment_flag" not in df_cls.columns:
        print("❌ Classification result missing 'predicted_curtailment_flag'.")
        return {"classification": cls_results, "regression": None, "combined": df_cls}

    df_for_reg = df_cls[df_cls["predicted_curtailment_flag"] == 1].copy()
    print_header("PIPELINE – ROWS FOR REGRESSION")
    print(f"Rows flagged as curtailment (1): {len(df_for_reg)}")

    # 3. Regression on filtered rows
    reg_results = predict_curtailment_regression(
        df_for_reg,
        model_path=reg_model_path,
        metadata_path=reg_metadata_path,
        plot=plot_reg,
    )

    # 4. Merge regression predictions back to classification df
    df_combined = df_cls.copy()
    df_combined["predicted_curtailment_kWh_per_kw"] = np.nan

    if reg_results is not None and not reg_results["predictions"].empty:

        df_reg_pred = reg_results["predictions"].copy()

        # 💡 MUST keep these keys for merge
        merge_keys = ["malo", "delivery_start_berlin"]

        # Only keep keys + prediction column
        df_reg_pred = df_reg_pred[merge_keys + ["predicted_curtailment_kWh_per_kw"]]

        # SAFE two-key merge
        df_combined = df_combined.merge(
            df_reg_pred,
            on=merge_keys,
            how="left",
            suffixes=("", "_reg"),
        )

        # Overwrite correct column
        df_combined["predicted_curtailment_kWh_per_kw"] = df_combined[
            "predicted_curtailment_kWh_per_kw_reg"
        ]

        df_combined.drop(columns=["predicted_curtailment_kWh_per_kw_reg"], inplace=True)

        df_combined["predicted_curtailment_kWh_per_kw"] = pd.to_numeric(
            df_combined["predicted_curtailment_kWh_per_kw"], errors="coerce"
        ).fillna(0)

    return {
        "classification": cls_results,
        "regression": reg_results,
        "combined": df_combined,
    }


# =============================================================================
# USAGE
# =============================================================================

# WIND_no_rules
# WIND_rules

# PV_no_rules
# PV_rules


if __name__ == "__main__":
    # df_new = pd.read_parquet("/data/datalore_ops_dev_bucket/TESTING_forecast_data_WIND_norules_fit0.parquet")
    df_new = pd.read_parquet(
        "/data/datalore_ops_dev_bucket/PV_NORULES/TESTING_forecast_data_PV_norules_7c_norules.parquet"
    )

    print(category)
    print("🦐🦐🦐🦐")
    print(df_new.columns)

    results = run_curtailment_forecast(
        df_new_prediction=df_new,
        cls_model_path=CLASS_MODEL_PATH,
        cls_metadata_path=CLASS_METADATA_PATH,
        reg_model_path=REG_MODEL_PATH,
        reg_metadata_path=REG_METADATA_PATH,
        plot_class=True,
        plot_reg=True,
    )

    if results is not None:
        df_out = results["combined"]
        print_header("FINAL COMBINED OUTPUT")
        print(f"Rows in output: {len(df_out)}")

        cols_show = [
            c
            for c in [
                "malo",
                "delivery_start_berlin",
                "predicted_curtailment_probability",
                "predicted_curtailment_flag",
                "predicted_curtailment_kWh_per_kw",
            ]
            if c in df_out.columns
        ]
        print(df_out[cols_show].head(15).round(4))

        # excel_file_path = "/data/datalore_ops_dev_bucket/WIND_NORULES/TESTING_result.xlsx"
        # rows_per_sheet = 1000000

        # with pd.ExcelWriter(excel_file_path, engine='openpyxl') as writer:
        #     for i in range(0, len(df_out), rows_per_sheet):
        #         chunk = df_out.iloc[i:i + rows_per_sheet]
        #         sheet_name = f'Pred_result_{i//rows_per_sheet + 1}'
        #         chunk.to_excel(writer, sheet_name=sheet_name, index=False)
        # print("🦐🦐🦐🦐")
