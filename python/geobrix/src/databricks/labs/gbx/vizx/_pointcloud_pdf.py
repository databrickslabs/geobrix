"""Multi-page PDF export for point-cloud pose renders (VizX).

``poses_to_pdf`` writes one page per pose from either a ``{name: Figure}``
dict (the return value of ``plot_point_cloud_poses``) or a directory of PNG
files. An optional title page is prepended when ``title`` is provided.

Uses matplotlib's ``PdfPages`` backend — no extra dependency beyond matplotlib.
Non-geo / plain-PDF only (GeoPDF / GDAL-tagged PDF is a future concern).
"""

from __future__ import annotations

import pathlib
from typing import Dict, Optional, Union

import matplotlib.figure


def poses_to_pdf(
    figs_or_dir: Union[Dict[str, matplotlib.figure.Figure], str, pathlib.Path],
    pdf_path: Union[str, pathlib.Path],
    *,
    title: Optional[str] = None,
    dpi: int = 200,
) -> pathlib.Path:
    """Write a multi-page PDF with one page per pose.

    Parameters
    ----------
    figs_or_dir:
        Either:

        - A ``dict[str, matplotlib.figure.Figure]`` — the return value of
          :func:`~databricks.labs.gbx.vizx._pointcloud_poses.plot_point_cloud_poses`.
          Pages are written in dict iteration order.
        - A directory path (str or :class:`pathlib.Path`) containing PNG files.
          Each PNG becomes one page, sorted by filename.

    pdf_path:
        Output path for the PDF file (local, UC Volume FUSE, or Workspace FUSE).
        Parent directories are created automatically.
    title:
        When provided, a plain text title page is prepended as the first page.
    dpi:
        Resolution in dots per inch for rasterising figure pages (default 200).
        Ignored when ``figs_or_dir`` is a directory (PNGs are embedded as images
        at their native resolution).

    Returns
    -------
    pathlib.Path
        The resolved path of the written PDF.
    """
    import matplotlib.pyplot as plt
    from matplotlib.backends.backend_pdf import PdfPages

    out = pathlib.Path(pdf_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    with PdfPages(str(out)) as pdf:
        # Optional title page
        if title is not None:
            title_fig = plt.figure(figsize=(8, 6))
            title_fig.text(
                0.5,
                0.5,
                title,
                ha="center",
                va="center",
                fontsize=20,
                wrap=True,
            )
            title_fig.patch.set_facecolor("white")
            pdf.savefig(title_fig, dpi=dpi, bbox_inches="tight")
            plt.close(title_fig)

        if isinstance(figs_or_dir, dict):
            # dict[str, Figure] path
            for name, fig in figs_or_dir.items():
                pdf.savefig(fig, dpi=dpi, bbox_inches="tight")
        else:
            # Directory of PNGs
            from PIL import Image as _Image

            src_dir = pathlib.Path(figs_or_dir)
            png_files = sorted(src_dir.glob("*.png"))
            for png in png_files:
                img_fig = plt.figure()
                ax = img_fig.add_axes([0, 0, 1, 1])
                ax.axis("off")
                with _Image.open(str(png)) as im:
                    ax.imshow(im)
                pdf.savefig(img_fig, dpi=dpi, bbox_inches="tight")
                plt.close(img_fig)

    return out
