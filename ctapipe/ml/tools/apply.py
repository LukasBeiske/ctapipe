"""
Tool to apply machine learning models in bulk (as opposed to event by event).
"""
import shutil

import numpy as np
import tables
from astropy.table.operations import hstack, vstack
from tqdm.auto import tqdm

from ctapipe.core.tool import Tool
from ctapipe.core.traits import Bool, Path, flag
from ctapipe.io import TableLoader, write_table
from ctapipe.io.tableio import TelListToMaskTransform

from ..sklearn import DispReconstructor, EnergyRegressor, ParticleIdClassifier
from ..stereo_combination import StereoCombiner


class ApplyModels(Tool):
    """Apply machine learning models to data.

    This tool predicts all events at once. To apply models in the
    regular event loop, set the appropriate options to ``ctapipe-process``.

    Models need to be trained with `~ctapipe.ml.tools.TrainEnergyRegressor`,
    `~ctapipe.ml.tools.TrainParticleIdClassifier` and
    `~ctapipe.ml.tools.TrainDispReconstructor`.
    """

    name = "ctapipe-ml-apply"
    description = __doc__

    overwrite = Bool(default_value=False).tag(config=True)

    input_url = Path(
        default_value=None,
        allow_none=False,
        directory_ok=False,
        exists=True,
    ).tag(config=True)

    output_path = Path(
        default_value=None,
        allow_none=False,
        directory_ok=False,
    ).tag(config=True)

    energy_regressor_path = Path(
        default_value=None,
        allow_none=True,
        exists=True,
        directory_ok=False,
    ).tag(config=True)

    particle_classifier_path = Path(
        default_value=None,
        allow_none=True,
        exists=True,
        directory_ok=False,
    ).tag(config=True)

    disp_models_path = Path(
        default_value=None,
        allow_none=True,
        exists=True,
        directory_ok=False,
    ).tag(config=True)

    aliases = {
        ("i", "input"): "ApplyModels.input_url",
        "energy-regressor": "ApplyModels.energy_regressor_path",
        "particle-classifier": "ApplyModels.particle_classifier_path",
        "disp-models": "ApplyModels.disp_models_path",
        ("o", "output"): "ApplyModels.output_path",
    }

    flags = {
        **flag(
            "overwrite",
            "ApplyModels.overwrite",
            "Overwrite tables in output file if it exists",
            "Don't overwrite tables in output file if it exists",
        ),
        "f": (
            {"ApplyModels": {"overwrite": True}},
            "Overwrite output file if it exists",
        ),
    }

    classes = [
        TableLoader,
        EnergyRegressor,
        ParticleIdClassifier,
        StereoCombiner,
    ]

    def setup(self):
        """
        Initialize components from config
        """
        self.log.info("Copying to output destination.")
        shutil.copy(self.input_url, self.output_path)

        self.h5file = tables.open_file(self.output_path, mode="r+")
        self.loader = TableLoader(
            parent=self,
            h5file=self.h5file,
            load_dl1_images=False,
            load_dl1_parameters=True,
            load_dl2=True,
            load_simulated=True,
            load_instrument=True,
            load_observation_info=True,
        )

        self.apply_regressor = self._setup_regressor()
        self.apply_classifier = self._setup_classifier()
        self.apply_geometry = self._setup_disp()

    def _setup_regressor(self):
        if self.energy_regressor_path is not None:
            self.energy_regressor = EnergyRegressor.read(
                self.energy_regressor_path,
                parent=self,
            )
            return True
        return False

    def _setup_classifier(self):
        if self.particle_classifier_path is not None:
            self.classifier = ParticleIdClassifier.read(
                self.particle_classifier_path,
                parent=self,
            )
            return True
        return False

    def _setup_disp(self):
        if self.disp_models_path is not None:
            self.disp_models = DispReconstructor.read(
                self.disp_models_path,
                parent=self,
            )
            return True
        return False

    def start(self):
        """Apply models to input tables"""
        if self.apply_regressor:
            self.log.info("Applying regressor to telescope events")
            mono_predictions = self._apply(self.energy_regressor, "energy")
            self.log.info(
                "Combining telescope-wise energy predictions to array event prediction"
            )
            self._combine(self.energy_regressor.stereo_combiner, mono_predictions)

        if self.apply_classifier:
            self.log.info("Applying classifier to telescope events")
            mono_predictions = self._apply(self.classifier, "classification")
            self.log.info(
                "Combining telescope-wise particle id predictions to array event prediction"
            )
            self._combine(self.classifier.stereo_combiner, mono_predictions)

        if self.apply_geometry:
            self.log.info("Applying disp models to telescope events.")
            mono_predictions = self._apply_disp()
            self.log.info(
                "Combining telescope-wise geometry predictions to array event prediction"
            )
            self._combine(self.disp_models.stereo_combiner, mono_predictions)

    def _apply(self, reconstructor, parameter):
        prefix = reconstructor.model_cls

        tel_tables = []

        desc = f"Applying {reconstructor.__class__.__name__}"
        unit = "telescope"
        for tel_id, tel in tqdm(self.loader.subarray.tel.items(), desc=desc, unit=unit):
            if tel not in reconstructor._models:
                self.log.warning(
                    "No model in %s for telescope type %s, skipping tel %d",
                    reconstructor,
                    tel,
                    tel_id,
                )
                continue

            table = self.loader.read_telescope_events([tel_id])
            if len(table) == 0:
                self.log.warning("No events for telescope %d", tel_id)
                continue

            table.remove_columns([c for c in table.colnames if c.startswith(prefix)])

            predictions = reconstructor.predict_table(tel, table)
            table = hstack(
                [table, predictions],
                join_type="exact",
                metadata_conflicts="ignore",
            )
            write_table(
                table[["obs_id", "event_id", "tel_id"] + predictions.colnames],
                self.loader.input_url,
                f"/dl2/event/telescope/{parameter}/{prefix}/tel_{tel_id:03d}",
                mode="a",
                overwrite=self.overwrite,
            )
            tel_tables.append(table)

        if len(tel_tables) == 0:
            raise ValueError("No predictions made for any telescope")

        return vstack(tel_tables)

    def _apply_disp(self):
        prefix = self.disp_models.prefix

        tables = []

        desc = "Applying disp models"
        unit = "telescope"
        for tel_id, tel in tqdm(self.loader.subarray.tel.items(), desc=desc, unit=unit):
            if tel not in self.disp_models._norm_models:
                self.log.warning(
                    "No disp regressor for telescope type %s, skipping tel %d",
                    tel,
                    tel_id,
                )
                continue
            if tel not in self.disp_models._sign_models:
                self.log.warning(
                    "No sign classifier for telescope type %s, skipping tel %d",
                    tel,
                    tel_id,
                )
                continue

            table = self.loader.read_telescope_events([tel_id])
            if len(table) == 0:
                self.log.warning("No events for telescope %d", tel_id)
                continue

            table.remove_columns([c for c in table.colnames if c.startswith(prefix)])

            disp_predictions, altaz_predictions = self.disp_models.predict_table(
                tel, table
            )
            table = hstack(
                [table, altaz_predictions, disp_predictions],
                join_type="exact",
                metadata_conflicts="ignore",
            )

            # tables should follow the container structure
            write_table(
                table[["obs_id", "event_id", "tel_id"] + altaz_predictions.colnames],
                self.loader.input_url,
                f"/dl2/event/telescope/geometry/{prefix}/tel_{tel_id:03d}",
                mode="a",
                overwrite=self.overwrite,
            )
            write_table(
                table[["obs_id", "event_id", "tel_id"] + disp_predictions.colnames],
                self.loader.input_url,
                f"/dl2/event/telescope/disp/{prefix}/tel_{tel_id:03d}",
                mode="a",
                overwrite=self.overwrite,
            )
            tables.append(table)

        if len(tables) == 0:
            raise ValueError("No predictions made for any telescope")

        return vstack(tables)

    def _combine(self, combiner, mono_predictions):
        stereo_predictions = combiner.predict_table(mono_predictions)

        trafo = TelListToMaskTransform(self.loader.subarray)
        for c in filter(
            lambda c: c.name.endswith("telescopes"),
            stereo_predictions.columns.values(),
        ):
            stereo_predictions[c.name] = np.array([trafo(r) for r in c])
            stereo_predictions[c.name].description = c.description

        write_table(
            stereo_predictions,
            self.loader.input_url,
            f"/dl2/event/subarray/{combiner.property}/{combiner.prefix}",
            mode="a",
            overwrite=self.overwrite,
        )

    def finish(self):
        """Close input file"""
        self.h5file.close()


def main():
    ApplyModels().run()


if __name__ == "__main__":
    main()
