import astropy.units as u
import numpy as np

from ctapipe.core import Tool
from ctapipe.core.traits import Bool, Int, Path, TraitError, flag
from ctapipe.io import TableLoader
from ctapipe.reco import CrossValidator, DispReconstructor
from ctapipe.reco.preprocessing import check_valid_rows, horizontal_to_telescope


class TrainDispReconstructor(Tool):
    """
    Tool to train a `~ctapipe.reco.DispReconstructor` on dl1b/dl2 data.

    The tool first performs a cross validation to give an initial estimate
    on the quality of the estimation and then finally trains two models
    (|disp| and sign(disp)) per telescope type on the full dataset.
    """

    name = "ctapipe-train-disp-reconstructor"
    description = __doc__

    examples = """
    ctapipe-train-disp-reconstructor \\
        --config train_disp_reconstructor.yaml \\
        --input gamma.dl2.h5 \\
        --output disp_models.pkl
    """

    output_path = Path(
        default_value=None,
        allow_none=False,
        directory_ok=False,
        help=(
            "Ouput path for the trained reconstructor."
            " At the moment, pickle is the only supported format.",
        ),
    ).tag(config=True)

    n_events = Int(
        default_value=None,
        allow_none=True,
        help=(
            "Number of events for training the models."
            " If not give, all available events will be used."
        ),
    ).tag(config=True)

    overwrite = Bool(help="overwrite existing output files").tag(config=True)

    random_seed = Int(
        default_value=0, help="Random seed for sampling and cross validation"
    ).tag(config=True)

    project_disp = Bool(
        default_value=False,
        help="Project true source position on main shower axis for true |disp| calculation",
    ).tag(config=True)

    flags = {
        **flag(
            "overwrite",
            "TrainDispReconstructor.overwrite",
            "Overwrite output existing output files",
            "Don't overwrite existing output files",
        ),
    }

    aliases = {
        ("i", "input"): "TableLoader.input_url",
        ("o", "output"): "TrainDispReconstructor.output_path",
        "cv-output": "CrossValidator.output_path",
    }

    classes = [TableLoader, DispReconstructor, CrossValidator]

    def setup(self):
        """
        Initialize components from config.
        """
        self.loader = TableLoader(
            parent=self,
            load_dl1_images=False,
            load_dl1_parameters=True,
            load_dl2=True,
            load_simulated=True,
            load_instrument=True,
            load_observation_info=True,
        )

        self.models = DispReconstructor(self.loader.subarray, parent=self)
        self.cross_validate = CrossValidator(parent=self, model_component=self.models)
        self.rng = np.random.default_rng(self.random_seed)

        if self.output_path.suffix != ".pkl":
            self.log.warning(
                "Expected .pkl extension for output_path, got %s",
                self.output_path.suffix,
            )

        if self.output_path.exists() and not self.overwrite:
            raise TraitError(
                f"output_path '{self.output_path}' exists and overwrite=False"
            )

    def start(self):
        """
        Train models per telescope type using a cross-validation.
        """
        types = self.loader.subarray.telescope_types
        self.log.info("Inputfile: %s", self.loader.input_url)

        self.log.info("Training models for %d types", len(types))
        for tel_type in types:
            self.log.info("Loading events for %s", tel_type)
            table = self._read_table(tel_type)

            self.log.info("Train models on %s events", len(table))
            self.cross_validate(tel_type, table)

            self.log.info("Performing final fit for %s", tel_type)
            self.models.fit(tel_type, table)
            self.log.info("done")

    def _read_table(self, telescope_type):
        table = self.loader.read_telescope_events([telescope_type])
        self.log.info("Events read from input: %d", len(table))

        mask = self.models.quality_query.get_table_mask(table)
        table = table[mask]
        self.log.info("Events after applying quality query: %d", len(table))

        table = self.models.feature_generator(table)

        true_norm, true_sign = self._get_true_disp(table)

        # Add true energy for energy-dependent performance plots
        table = table[self.models.features + ["true_energy"]]
        table[self.models.norm_target] = true_norm
        table[self.models.sign_target] = true_sign

        valid = check_valid_rows(table)
        if np.any(~valid):
            self.log.warning("Dropping non-predicable events.")
            table = table[valid]

        if self.n_events is not None:
            n_events = min(self.n_events, len(table))
            idx = self.rng.choice(len(table), n_events, replace=False)
            idx.sort()
            table = table[idx]

        return table

    def _get_true_disp(self, table):
        fov_lon, fov_lat = horizontal_to_telescope(
            alt=table["true_alt"],
            az=table["true_az"],
            pointing_alt=table["subarray_pointing_lat"],
            pointing_az=table["subarray_pointing_lon"],
        )

        # numpy's trigonometric functions need radians
        psi = table["hillas_psi"].quantity.to_value(u.rad)
        cog_lon = table["hillas_fov_lon"].quantity
        cog_lat = table["hillas_fov_lat"].quantity

        delta_lon = fov_lon - cog_lon
        delta_lat = fov_lat - cog_lat

        true_disp = np.cos(psi) * delta_lon + np.sin(psi) * delta_lat
        true_sign = np.sign(true_disp)

        if self.project_disp:
            true_norm = np.abs(true_disp)
        else:
            true_norm = np.sqrt((fov_lon - cog_lon) ** 2 + (fov_lat - cog_lat) ** 2)

        return true_norm, true_sign.astype(np.int8)

    def finish(self):
        """
        Write-out trained models and cross-validation results.
        """
        self.log.info("Writing output")
        self.models.write(self.output_path, overwrite=self.overwrite)
        if self.cross_validate.output_path:
            self.cross_validate.write(overwrite=self.overwrite)
        self.loader.close()


def main():
    TrainDispReconstructor().run()


if __name__ == "__main__":
    main()
