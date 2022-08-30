import astropy.units as u
import numpy as np
import pytest
from astropy.table import Table
from numpy.testing import assert_allclose, assert_array_equal

from ctapipe.containers import (
    ArrayEventContainer,
    HillasParametersContainer,
    ImageParametersContainer,
    ParticleClassificationContainer,
    ReconstructedContainer,
    ReconstructedEnergyContainer,
    ReconstructedGeometryContainer,
)
from ctapipe.ml.stereo_combination import StereoMeanCombiner


@pytest.fixture(scope="module")
def mono_table():
    """
    Dummy table of telescope events with a
    prediction and weights.
    """
    return Table(
        {
            "obs_id": [1, 1, 1, 1, 1, 2],
            "event_id": [1, 1, 1, 2, 2, 1],
            "tel_id": [1, 2, 3, 5, 7, 1],
            "hillas_intensity": [1, 2, 0, 1, 5, 9],
            "hillas_width": [0.1, 0.2, 0.1, 0.1, 0.2, 0.1] * u.deg,
            "hillas_length": 3 * ([0.1, 0.2, 0.1, 0.1, 0.2, 0.1] * u.deg),
            "dummy_tel_energy": [1, 10, 4, 0.5, 0.7, 1] * u.TeV,
            "dummy_tel_is_valid": [
                True,
                True,
                True,
                True,
                False,
                False,
            ],
            "classifier_tel_prediction": [1, 0, 0.5, 0, 0.6, 1],
            "classifier_tel_is_valid": [
                True,
                True,
                False,
                True,
                True,
                True,
            ],
            "disp_regressor_sign_classifier_alt": [58.5, 58, 62.5, 72, 74.5, 81]
            * u.deg,
            "disp_regressor_sign_classifier_az": [12.5, 15, 13, 21, 20, 14.5] * u.deg,
            "disp_regressor_sign_classifier_is_valid": [
                True,
                False,
                True,
                True,
                True,
                True,
            ],
        }
    )


@pytest.mark.parametrize("weights", ["konrad", "intensity", "none"])
def test_predict_mean_energy(weights, mono_table):
    combine = StereoMeanCombiner(
        algorithm=["dummy"],
        combine_property="energy",
        weights=weights,
    )
    stereo = combine.predict_table(mono_table)
    assert stereo.colnames == [
        "obs_id",
        "event_id",
        "dummy_energy",
        "dummy_energy_uncert",
        "dummy_is_valid",
        "dummy_goodness_of_fit",
        "dummy_telescopes",
    ]
    assert_array_equal(stereo["obs_id"], np.array([1, 1, 2]))
    assert_array_equal(stereo["event_id"], np.array([1, 2, 1]))
    if weights == "intensity":
        assert_array_equal(stereo["dummy_energy"], [7, 0.5, np.nan] * u.TeV)
    elif weights == "none":
        assert_array_equal(stereo["dummy_energy"], [5, 0.5, np.nan] * u.TeV)

    assert_array_equal(stereo["dummy_telescopes"][0], np.array([1, 2, 3]))
    assert_array_equal(stereo["dummy_telescopes"][1], 5)


def test_predict_mean_classification(mono_table):
    combine = StereoMeanCombiner(
        algorithm=["classifier"],
        combine_property="classification",
    )
    stereo = combine.predict_table(mono_table)
    assert stereo.colnames == [
        "obs_id",
        "event_id",
        "classifier_prediction",
        "classifier_is_valid",
        "classifier_goodness_of_fit",
        "classifier_telescopes",
    ]
    assert_array_equal(stereo["obs_id"], np.array([1, 1, 2]))
    assert_array_equal(stereo["event_id"], np.array([1, 2, 1]))
    assert_array_equal(
        stereo["classifier_prediction"],
        [0.5, 0.3, 1],
    )
    tel_ids = stereo["classifier_telescopes"]
    assert_array_equal(tel_ids[0], [1, 2])
    assert_array_equal(tel_ids[1], [5, 7])
    assert_array_equal(tel_ids[2], [1])


def test_predict_mean_disp(mono_table):
    combine = StereoMeanCombiner(
        algorithm=["disp_regressor", "sign_classifier"],
        combine_property="geometry",
    )
    stereo = combine.predict(mono_table)
    assert stereo.colnames == [
        "obs_id",
        "event_id",
        "disp_regressor_sign_classifier_alt",
        "disp_regressor_sign_classifier_alt_uncert",
        "disp_regressor_sign_classifier_az",
        "disp_regressor_sign_classifier_az_uncert",
        "disp_regressor_sign_classifier_is_valid",
        "disp_regressor_sign_classifier_goodness_of_fit",
        "disp_regressor_sign_classifier_tel_ids",
    ]
    assert_array_equal(stereo["obs_id"], np.array([1, 1, 2]))
    assert_array_equal(stereo["event_id"], np.array([1, 2, 1]))
    assert_allclose(
        stereo["disp_regressor_sign_classifier_alt"].quantity,
        [60.5002328, 73.2505989, 81] * u.deg,
        atol=1e-7,
    )
    assert_allclose(
        stereo["disp_regressor_sign_classifier_az"].quantity,
        [12.7345693, 20.5362510, 14.5] * u.deg,
        atol=1e-7,
    )
    tel_ids = stereo["disp_regressor_sign_classifier_tel_ids"]
    assert_array_equal(tel_ids[0], [1, 3])
    assert_array_equal(tel_ids[1], [5, 7])
    assert_array_equal(tel_ids[2], [1])


@pytest.mark.parametrize("weights", ["konrad", "intensity", "none"])
def test_mean_prediction_single_event(weights):
    event = ArrayEventContainer()

    for tel_id, intensity in zip((25, 125, 130), (100, 200, 400)):
        event.dl1.tel[tel_id].parameters = ImageParametersContainer(
            hillas=HillasParametersContainer(
                intensity=intensity,
                width=0.1 * u.deg,
                length=0.3 * u.deg,
            )
        )

    event.dl2.tel[25] = ReconstructedContainer(
        energy={
            "dummy": ReconstructedEnergyContainer(energy=10 * u.GeV, is_valid=True)
        },
        classification={
            "dummy": ParticleClassificationContainer(prediction=1.0, is_valid=True)
        },
        geometry={
            "dummy0_dummy1": ReconstructedGeometryContainer(
                alt=60 * u.deg, az=15 * u.deg, is_valid=True
            )
        },
    )
    event.dl2.tel[125] = ReconstructedContainer(
        energy={
            "dummy": ReconstructedEnergyContainer(energy=20 * u.GeV, is_valid=True)
        },
        classification={
            "dummy": ParticleClassificationContainer(prediction=0.0, is_valid=True)
        },
        geometry={
            "dummy0_dummy1": ReconstructedGeometryContainer(
                alt=50 * u.deg, az=30 * u.deg, is_valid=True
            )
        },
    )
    event.dl2.tel[130] = ReconstructedContainer(
        energy={
            "dummy": ReconstructedEnergyContainer(energy=0.04 * u.TeV, is_valid=True)
        },
        classification={
            "dummy": ParticleClassificationContainer(prediction=0.8, is_valid=True)
        },
        geometry={
            "dummy0_dummy1": ReconstructedGeometryContainer(
                alt=45 * u.deg, az=280 * u.deg, is_valid=True
            )
        },
    )

    combine_energy = StereoMeanCombiner(
        algorithm=["dummy"],
        combine_property="energy",
        weights=weights,
    )
    combine_classification = StereoMeanCombiner(
        algorithm=["dummy"],
        combine_property="classification",
        weights=weights,
    )
    combine_geometry = StereoMeanCombiner(
        algorithm=["dummy0", "dummy1"],
        combine_property="geometry",
        weights=weights,
    )
    combine_energy(event)
    combine_classification(event)
    combine_geometry(event)
    if weights == "none":
        assert u.isclose(event.dl2.stereo.energy["dummy"].energy, (70 / 3) * u.GeV)
        assert u.isclose(
            event.dl2.stereo.geometry["dummy0_dummy1"].alt, 63.0738383 * u.deg
        )
        assert u.isclose(
            event.dl2.stereo.geometry["dummy0_dummy1"].az, 348.0716693 * u.deg
        )
    elif weights == "intensity":
        assert u.isclose(event.dl2.stereo.energy["dummy"].energy, 30 * u.GeV)
        assert u.isclose(
            event.dl2.stereo.geometry["dummy0_dummy1"].alt, 60.9748605 * u.deg
        )
        assert u.isclose(
            event.dl2.stereo.geometry["dummy0_dummy1"].az, 316.0365515 * u.deg
        )
    assert event.dl2.stereo.classification["dummy"].prediction == 0.6
