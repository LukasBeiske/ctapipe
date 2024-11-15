"""Components to generate IRFs"""

from abc import abstractmethod

import astropy.units as u
import numpy as np
from astropy.io.fits import BinTableHDU
from astropy.table import QTable
from pyirf.io import (
    create_aeff2d_hdu,
    create_background_2d_hdu,
    create_energy_dispersion_hdu,
    create_psf_table_hdu,
)
from pyirf.irf import (
    background_2d,
    effective_area_per_energy,
    effective_area_per_energy_and_fov,
    energy_dispersion,
    psf_table,
)
from pyirf.simulations import SimulatedEventsInfo

from ..core.traits import AstroQuantity, Bool, Dict, Float, Integer, List
from ._gammapy_map_axis import MapAxes, MapAxis
from .binning import DefaultFoVOffsetBins, DefaultRecoEnergyBins, DefaultTrueEnergyBins


class PsfMakerBase(DefaultTrueEnergyBins):
    """Base class for calculating the point spread function."""

    def __init__(self, parent=None, **kwargs):
        super().__init__(parent=parent, **kwargs)

    @abstractmethod
    def make_psf_hdu(self, events: QTable, extname: str = "PSF") -> BinTableHDU:
        """
        Calculate the psf and create a fits binary table HDU in GADF format.

        Parameters
        ----------
        events: astropy.table.QTable
            Reconstructed events to be used.
        extname: str
            Name for the BinTableHDU.

        Returns
        -------
        BinTableHDU
        """


class BackgroundRateMakerBase(DefaultRecoEnergyBins):
    """Base class for calculating the background rate."""

    def __init__(self, parent=None, **kwargs):
        super().__init__(parent=parent, **kwargs)

    @abstractmethod
    def make_bkg_hdu(
        self, events: QTable, obs_time: u.Quantity, extname: str = "BACKGROUND"
    ) -> BinTableHDU:
        """
        Calculate the background rate and create a fits binary table HDU
        in GADF format.

        Parameters
        ----------
        events: astropy.table.QTable
            Reconstructed events to be used.
        obs_time: astropy.units.Quantity[time]
            Observation time. This must match with how the individual event
            weights are calculated.
        extname: str
            Name for the BinTableHDU.

        Returns
        -------
        BinTableHDU
        """


class EnergyDispersionMakerBase(DefaultTrueEnergyBins):
    """Base class for calculating the energy dispersion."""

    energy_migration_min = Float(
        help="Minimum value of energy migration ratio",
        default_value=0.2,
    ).tag(config=True)

    energy_migration_max = Float(
        help="Maximum value of energy migration ratio",
        default_value=5,
    ).tag(config=True)

    energy_migration_n_bins = Integer(
        help="Number of bins in log scale for energy migration ratio",
        default_value=30,
    ).tag(config=True)

    energy_migration_linear_bins = Bool(
        help="Bin energy migration ratio using linear bins",
        default_value=False,
    ).tag(config=True)

    def __init__(self, parent=None, **kwargs):
        super().__init__(parent=parent, **kwargs)
        bin_func = np.geomspace
        if self.energy_migration_linear_bins:
            bin_func = np.linspace
        self.migration_bins = bin_func(
            self.energy_migration_min,
            self.energy_migration_max,
            self.energy_migration_n_bins + 1,
        )

    @abstractmethod
    def make_edisp_hdu(
        self, events: QTable, point_like: bool, extname: str = "ENERGY MIGRATION"
    ) -> BinTableHDU:
        """
        Calculate the energy dispersion and create a fits binary table HDU
        in GADF format.

        Parameters
        ----------
        events: astropy.table.QTable
            Reconstructed events to be used.
        point_like: bool
            If a direction cut was applied on ``events``, pass ``True``, else ``False``
            for a full-enclosure energy dispersion.
        extname: str
            Name for the BinTableHDU.

        Returns
        -------
        BinTableHDU
        """


class EffectiveAreaMakerBase(DefaultTrueEnergyBins):
    """Base class for calculating the effective area."""

    def __init__(self, parent=None, **kwargs):
        super().__init__(parent=parent, **kwargs)

    @abstractmethod
    def make_aeff_hdu(
        self,
        events: QTable,
        point_like: bool,
        signal_is_point_like: bool,
        sim_info: SimulatedEventsInfo,
        extname: str = "EFFECTIVE AREA",
    ) -> BinTableHDU:
        """
        Calculate the effective area and create a fits binary table HDU
        in GADF format.

        Parameters
        ----------
        events: astropy.table.QTable
            Reconstructed events to be used.
        point_like: bool
            If a direction cut was applied on ``events``, pass ``True``, else ``False``
            for a full-enclosure effective area.
        signal_is_point_like: bool
            If ``events`` were simulated only at a single point in the field of view,
            pass ``True``, else ``False``.
        sim_info: pyirf.simulations.SimulatedEventsInfoa
            The overall statistics of the simulated events.
        extname: str
            Name of the BinTableHDU.

        Returns
        -------
        BinTableHDU
        """


class EffectiveAreaMakerTest(EffectiveAreaMakerBase):
    axes = List(
        Dict(),
        default_value=[
            dict(
                name="energy_true",
                unit="TeV",
                low=0.01,
                high=500,
                n_bins=40,
                spacing="log",
            ),
            dict(name="fov_theta", unit="deg", low=0, high=5, n_bins=5, spacing="lin"),
        ],
    ).tag(config=True)

    def __init__(self, parent=None, **kwargs):
        super().__init__(parent, **kwargs)

        mapaxes = []
        for ax in self.axes:
            if ax["spacing"] == "log":
                spacing_func = np.logspace
                ax["low"] = np.log10(ax["low"])
                ax["high"] = np.log10(ax["high"])
            elif ax["spacing"] == "lin":
                spacing_func = np.linspace
            else:
                raise NotImplementedError()

            mapaxes.append(
                MapAxis(
                    nodes=spacing_func(ax["low"], ax["high"], ax["n_bins"] + 1),
                    interp=ax["spacing"],
                    name=ax["name"],
                    unit=ax["unit"],
                )
            )

        self.mapaxes = MapAxes(mapaxes)

    def make_aeff_hdu(
        self,
        events: QTable,
        point_like: bool,
        signal_is_point_like: bool,
        sim_info: SimulatedEventsInfo,
        extname: str = "EFFECTIVE AREA",
    ) -> BinTableHDU:
        if np.all(np.sort(self.mapaxes.names) == ["energy_true", "fov_theta"]):
            # For point-like gammas the effective area can only be calculated
            # at one point in the FoV.
            if signal_is_point_like:
                aeff = effective_area_per_energy(
                    selected_events=events,
                    simulation_info=sim_info,
                    true_energy_bins=self.mapaxes["energy_true"].edges,
                )
                # +1 dimension for FOV offset
                aeff = aeff[..., np.newaxis]
            else:
                aeff = effective_area_per_energy_and_fov(
                    selected_events=events,
                    simulation_info=sim_info,
                    true_energy_bins=self.mapaxes["energy_true"].edges,
                    fov_offset_bins=self.mapaxes["fov_theta"].edges,
                )

            return create_aeff2d_hdu(
                effective_area=aeff,
                true_energy_bins=self.mapaxes["energy_true"].edges,
                fov_offset_bins=self.mapaxes["fov_theta"].edges,
                point_like=point_like,
                extname=extname,
            )
        else:
            raise NotImplementedError()


class EffectiveArea2dMaker(EffectiveAreaMakerBase, DefaultFoVOffsetBins):
    """
    Creates a radially symmetric parameterization of the effective area in equidistant
    bins of logarithmic true energy and field of view offset.
    """

    def __init__(self, parent=None, **kwargs):
        super().__init__(parent=parent, **kwargs)

    def make_aeff_hdu(
        self,
        events: QTable,
        point_like: bool,
        signal_is_point_like: bool,
        sim_info: SimulatedEventsInfo,
        extname: str = "EFFECTIVE AREA",
    ) -> BinTableHDU:
        # For point-like gammas the effective area can only be calculated
        # at one point in the FoV.
        if signal_is_point_like:
            aeff = effective_area_per_energy(
                selected_events=events,
                simulation_info=sim_info,
                true_energy_bins=self.true_energy_bins,
            )
            # +1 dimension for FOV offset
            aeff = aeff[..., np.newaxis]
        else:
            aeff = effective_area_per_energy_and_fov(
                selected_events=events,
                simulation_info=sim_info,
                true_energy_bins=self.true_energy_bins,
                fov_offset_bins=self.fov_offset_bins,
            )

        return create_aeff2d_hdu(
            effective_area=aeff,
            true_energy_bins=self.true_energy_bins,
            fov_offset_bins=self.fov_offset_bins,
            point_like=point_like,
            extname=extname,
        )


class EnergyDispersion2dMaker(EnergyDispersionMakerBase, DefaultFoVOffsetBins):
    """
    Creates a radially symmetric parameterization of the energy dispersion in
    equidistant bins of logarithmic true energy and field of view offset.
    """

    def __init__(self, parent=None, **kwargs):
        super().__init__(parent=parent, **kwargs)

    def make_edisp_hdu(
        self, events: QTable, point_like: bool, extname: str = "ENERGY DISPERSION"
    ) -> BinTableHDU:
        edisp = energy_dispersion(
            selected_events=events,
            true_energy_bins=self.true_energy_bins,
            fov_offset_bins=self.fov_offset_bins,
            migration_bins=self.migration_bins,
        )
        return create_energy_dispersion_hdu(
            energy_dispersion=edisp,
            true_energy_bins=self.true_energy_bins,
            migration_bins=self.migration_bins,
            fov_offset_bins=self.fov_offset_bins,
            point_like=point_like,
            extname=extname,
        )


class BackgroundRate2dMaker(BackgroundRateMakerBase, DefaultFoVOffsetBins):
    """
    Creates a radially symmetric parameterization of the background rate in equidistant
    bins of logarithmic reconstructed energy and field of view offset.
    """

    def __init__(self, parent=None, **kwargs):
        super().__init__(parent=parent, **kwargs)

    def make_bkg_hdu(
        self, events: QTable, obs_time: u.Quantity, extname: str = "BACKGROUND"
    ) -> BinTableHDU:
        background_rate = background_2d(
            events=events,
            reco_energy_bins=self.reco_energy_bins,
            fov_offset_bins=self.fov_offset_bins,
            t_obs=obs_time,
        )
        return create_background_2d_hdu(
            background_2d=background_rate,
            reco_energy_bins=self.reco_energy_bins,
            fov_offset_bins=self.fov_offset_bins,
            extname=extname,
        )


class Psf3dMaker(PsfMakerBase, DefaultFoVOffsetBins):
    """
    Creates a radially symmetric point spread function calculated in equidistant bins
    of source offset, logarithmic true energy, and field of view offset.
    """

    source_offset_min = AstroQuantity(
        help="Minimum value for Source offset",
        default_value=u.Quantity(0, u.deg),
        physical_type=u.physical.angle,
    ).tag(config=True)

    source_offset_max = AstroQuantity(
        help="Maximum value for Source offset",
        default_value=u.Quantity(1, u.deg),
        physical_type=u.physical.angle,
    ).tag(config=True)

    source_offset_n_bins = Integer(
        help="Number of bins for Source offset",
        default_value=100,
    ).tag(config=True)

    def __init__(self, parent=None, **kwargs):
        super().__init__(parent=parent, **kwargs)
        self.source_offset_bins = u.Quantity(
            np.linspace(
                self.source_offset_min.to_value(u.deg),
                self.source_offset_max.to_value(u.deg),
                self.source_offset_n_bins + 1,
            ),
            u.deg,
        )

    def make_psf_hdu(self, events: QTable, extname: str = "PSF") -> BinTableHDU:
        psf = psf_table(
            events=events,
            true_energy_bins=self.true_energy_bins,
            fov_offset_bins=self.fov_offset_bins,
            source_offset_bins=self.source_offset_bins,
        )
        return create_psf_table_hdu(
            psf=psf,
            true_energy_bins=self.true_energy_bins,
            fov_offset_bins=self.fov_offset_bins,
            source_offset_bins=self.source_offset_bins,
            extname=extname,
        )