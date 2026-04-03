"""
Generate synthetic policy documents for portfolio demo.
Creates PDF-like text content for embedding into LanceDB.
"""

import os
from datetime import datetime

try:
    from .synthetic_config import SYNTHETIC_DOCUMENTS
except ImportError:
    from synthetic_config import SYNTHETIC_DOCUMENTS


def generate_document_content(doc_config):
    """Generate full text content for a synthetic document."""
    title = doc_config['title']
    doc_type = doc_config['type']
    sections = doc_config['sections']

    # Header
    lines = [
        f"{'='*60}",
        f"{title.upper()}",
        f"PT TELKOM INDONESIA",
        f"Versi: 1.0 | Tanggal: {datetime.now().strftime('%d %B %Y')}",
        f"{'='*60}",
        ""
    ]

    # Table of contents
    lines.append("DAFTAR ISI")
    lines.append("-" * 40)
    for i, section in enumerate(sections, 1):
        lines.append(f"{i}. {section}")
    lines.append("")
    lines.append("")

    # Content sections
    for i, section in enumerate(sections, 1):
        lines.append(f"{i}. {section}")
        lines.append("-" * 40)
        lines.extend(generate_section_content(section, doc_type))
        lines.append("")
        lines.append("")

    # Footer
    lines.extend([
        "",
        "=" * 60,
        "Dokumen ini dibuat untuk keperluan demonstrasi.",
        "PT Telkom Indonesia - Divisi IndiHome",
        f"(c) {datetime.now().year} PT Telekomunikasi Indonesia Tbk",
        "=" * 60
    ])

    return '\n'.join(lines)


def generate_section_content(section_title, doc_type):
    """Generate realistic content for a section based on type."""
    # Content templates based on document type
    templates = {
        'faq': {
            'Apa itu IndiHome?': [
                "IndiHome adalah layanan internet fiber optic rumahan dari Telkom Indonesia.",
                "Layanan ini menyediakan koneksi internet berkecepatan tinggi untuk kebutuhan rumah tangga.",
                "Paket IndiHome tersedia dalam berbagai kecepatan mulai dari 10 Mbps hingga 200 Mbps.",
                "IndiHome juga menyediakan layanan tambahan seperti IndiHome TV dan telepon rumah."
            ],
            'Apa saja paket IndiHome yang tersedia?': [
                "Paket IndiHome Lite: 10 Mbps - Rp199.000/bulan",
                "Paket IndiHome Value: 20 Mbps - Rp299.000/bulan",
                "Paket IndiHome Max: 30 Mbps - Rp399.000/bulan",
                "Paket IndiHome Ultra: 50 Mbps - Rp599.000/bulan",
                "Paket IndiHome Supreme: 100 Mbps - Rp899.000/bulan",
                "Paket IndiHome Ultimate: 200 Mbps - Rp1.299.000/bulan"
            ],
            'default': [
                f"Pertanyaan: {section_title}",
                f"Jawaban: Informasi lengkap mengenai {section_title.lower()} dapat ditemukan di bagian ini.",
                "Untuk informasi lebih lanjut, silakan hubungi customer service IndiHome di 147.",
                "Atau kunjungi website resmi di www.indihome.co.id"
            ]
        },
        'sop': {
            'default': [
                f"Prosedur: {section_title}",
                "",
                "Langkah-langkah yang harus dilakukan:",
                "1. Persiapan: Pastikan semua persyaratan telah dipenuhi",
                "2. Pelaksanaan: Ikuti prosedur standar yang telah ditetapkan",
                "3. Dokumentasi: Catat semua langkah yang dilakukan",
                "4. Verifikasi: Konfirmasi hasil dengan supervisor",
                "5. Serah terima: Dokumen hasil kepada pihak terkait",
                "",
                f"Estimasi waktu: {(len(section_title) % 5) + 1} jam kerja",
                f"Penanggung jawab: Tim {section_title.split()[0] if ' ' in section_title else 'Operasional'}"
            ]
        },
        'policy': {
            'default': [
                f"Kebijakan: {section_title}",
                "",
                "Ketentuan:",
                "1. Kebijakan ini berlaku untuk semua pelanggan IndiHome",
                "2. Berlaku mulai dari tanggal publikasi dokumen ini",
                "3. Perubahan kebijakan akan diinformasikan 30 hari sebelumnya",
                "",
                "Pelanggaran kebijakan dapat mengakibatkan:",
                "- Teguran tertulis",
                "- Denda sesuai ketentuan",
                "- Pemblokiran layanan sementara",
                "- Pemutusan kontrak dalam kasus berat",
                "",
                "Untuk pertanyaan, hubungi customer service"
            ]
        },
        'guide': {
            'default': [
                f"Panduan: {section_title}",
                "",
                "Langkah-langkah:",
                "1. Pastikan perangkat dalam kondisi menyala",
                "2. Ikuti instruksi di layar atau panel",
                "3. Tunggu proses selesai",
                "4. Verifikasi hasil konfigurasi",
                "",
                "Tips:",
                "- Baca manual perangkat sebelum memulai",
                "- Jangan memodifikasi pengaturan yang tidak dipahami",
                "- Hubungi teknisi jika mengalami kendala",
                "",
                "Peringatan:",
                "Kesalahan konfigurasi dapat mengakibatkan kerusakan perangkat."
            ]
        },
        'legal': {
            'default': [
                f"Pasal: {section_title}",
                "",
                "Dengan menggunakan layanan IndiHome, pelanggan setuju untuk:",
                "1. Mematuhi semua syarat dan ketentuan yang berlaku",
                "2. Tidak menyalahgunakan layanan untuk aktivitas ilegal",
                "3. Membayar tagihan tepat waktu",
                "4. Menjaga kerahasiaan informasi akun",
                "",
                "PT Telkom Indonesia berhak untuk:",
                "- Mengubah syarat dan ketentuan dengan pemberitahuan terlebih dahulu",
                "- Menolak layanan kepada pelanggan yang melanggar ketentuan",
                "- Menuntut ganti rugi atas kerugian yang ditimbulkan",
                "",
                "Hukum yang berlaku: Hukum Republik Indonesia"
            ]
        },
        'info': {
            'default': [
                f"Informasi: {section_title}",
                "",
                "Detail:",
                "Coverage area tersedia di berbagai wilayah Indonesia.",
                "Untuk mengecek ketersediaan layanan di lokasi Anda:",
                "",
                "1. Kunjungi www.indihome.co.id",
                "2. Masukkan alamat atau kode pos",
                "3. Sistem akan menampilkan paket yang tersedia",
                "",
                "Jika lokasi belum tercover:",
                "- Daftarkan lokasi Anda untuk notifikasi ekspansi",
                "- Hubungi sales representative untuk alternatif layanan"
            ]
        }
    }

    # Get content for this section
    type_templates = templates.get(doc_type, templates['policy'])
    return type_templates.get(section_title, type_templates['default'])


def generate_all_documents(output_dir):
    """Generate all synthetic documents as text files."""
    os.makedirs(output_dir, exist_ok=True)

    generated_files = []

    for doc_config in SYNTHETIC_DOCUMENTS:
        original_filename = doc_config['filename']
        output_filename = f"{os.path.splitext(original_filename)[0]}.txt"
        filepath = os.path.join(output_dir, output_filename)

        content = generate_document_content(doc_config)

        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)

        generated_files.append({
            'filepath': filepath,
            'filename': output_filename,
            'source_filename': original_filename,
            'title': doc_config['title'],
            'type': doc_config['type']
        })
        print(f"Generated: {filepath}")

    return generated_files


if __name__ == '__main__':
    output_dir = os.path.join(os.path.dirname(__file__), 'documents')
    files = generate_all_documents(output_dir)
    print(f"\nGenerated {len(files)} documents in {output_dir}")
