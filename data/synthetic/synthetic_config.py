"""
Configuration for synthetic data generation.
Realistic distributions for Indonesian ISP market.
"""

# Provider distribution (realistic market share)
PROVIDERS = {
    "IndiHome": 0.40,
    "First Media": 0.20,
    "MyRepublic": 0.15,
    "CBN": 0.10,
    "Biznet": 0.10,
    "Iconnet": 0.05
}

# Speed tiers with distribution
SPEED_TIERS = {
    10: 0.05,   # Entry level
    20: 0.15,   # Budget
    30: 0.30,   # Most common
    50: 0.25,   # Mid-tier
    100: 0.15,  # Premium
    200: 0.10   # High-end
}

# Price ranges by speed (in IDR, thousands)
PRICE_BY_SPEED = {
    10: (150, 250),
    20: (200, 350),
    30: (250, 450),
    50: (350, 650),
    100: (500, 950),
    200: (800, 1500)
}

# Location distribution (Indonesian population weights)
LOCATIONS = {
    "Jakarta": 0.30,
    "Surabaya": 0.15,
    "Bandung": 0.12,
    "Medan": 0.10,
    "Semarang": 0.08,
    "Bali": 0.07,
    "Makassar": 0.05,
    "Palembang": 0.05,
    "Tangerang": 0.04,
    "Depok": 0.04
}

# Product name templates by provider
PRODUCT_NAMES = {
    "IndiHome": ["IndiHome {speed}Mbps", "IndiHome {speed}Mbps Premium", "IndiHome {speed}Mbps Gaming"],
    "First Media": ["FastNet {speed}Mbps", "First Media {speed}Mbps"],
    "MyRepublic": ["MyRepublic {speed}Mbps", "MyRepublic Gamer {speed}Mbps"],
    "CBN": ["CBN Link {speed}Mbps", "CBN Business {speed}Mbps"],
    "Biznet": ["Biznet Home {speed}Mbps", "Biznet Metronet {speed}Mbps"],
    "Iconnet": ["Iconnet {speed}Mbps", "Iconnet Plus {speed}Mbps"]
}

# Promotional gimmicks
GIMMICKS = [
    "Free installation",
    "Free router rental",
    "Free 1 month subscription",
    "Free Netflix subscription 3 months",
    "Free gaming bonus",
    "Upgrade promo next month",
    "Loyalty discount 10%",
    "Family package discount",
    "Student discount available",
    "Corporate rate available"
]

# Sources
SOURCES = [
    "official_website",
    "promo_campaign",
    "partner_portal",
    "agent_network"
]

# Number of records per table
NUM_PRODUCT_RECORDS = 500
NUM_PROVIDER_RECORDS = 500

# Synthetic documents configuration
SYNTHETIC_DOCUMENTS = [
    {
        "filename": "faq_indihome.md",
        "title": "FAQ IndiHome - Pertanyaan Umum",
        "type": "faq",
        "sections": [
            "Cara berlangganan IndiHome",
            "Jenis paket yang tersedia",
            "Proses instalasi",
            "Pembayaran dan tagihan",
            "Gangguan dan keluhan",
            "Upgrade dan downgrade paket"
        ]
    },
    {
        "filename": "faq_broadband.md",
        "title": "FAQ Broadband Internet",
        "type": "faq",
        "sections": [
            "Apa itu broadband",
            "Perbedaan speed tier",
            "Kecepatan upload vs download",
            "FUP (Fair Usage Policy)",
            "IP Public dan Static IP",
            "Latency dan ping"
        ]
    },
    {
        "filename": "sop_instalasi.md",
        "title": "SOP Instalasi - Prosedur Standar",
        "type": "sop",
        "sections": [
            "Persiapan sebelum instalasi",
            "Survey lokasi",
            "Pemasangan kabel",
            "Konfigurasi router",
            "Testing koneksi",
            "Serah terima pelanggan"
        ]
    },
    {
        "filename": "sop_maintenance.md",
        "title": "SOP Maintenance - Prosedur Pemeliharaan",
        "type": "sop",
        "sections": [
            "Jadwal maintenance rutin",
            "Prosedur preventive maintenance",
            "Troubleshooting standar",
            "Eskalasi masalah",
            "Dokumentasi kerusakan",
            "Penggantian perangkat"
        ]
    },
    {
        "filename": "policy_billing.md",
        "title": "Kebijakan Billing dan Pembayaran",
        "type": "policy",
        "sections": [
            "Metode pembayaran",
            "Tanggal jatuh tempo",
            "Denda keterlambatan",
            "Pembayaran di muka",
            "Invoice dan tagihan",
            "Cara cek tagihan"
        ]
    },
    {
        "filename": "policy_refund.md",
        "title": "Kebijakan Refund dan Pembatalan",
        "type": "policy",
        "sections": [
            "Syarat refund",
            "Proses pengajuan refund",
            "Waktu pemrosesan refund",
            "Pembatalan berlangganan",
            "Potongan administrasi",
            "Refund deposit"
        ]
    },
    {
        "filename": "policy_sla.md",
        "title": "Service Level Agreement (SLA)",
        "type": "policy",
        "sections": [
            "Uptime guarantee",
            "Response time pelanggan",
            "Resolution time",
            "Kompensasi jika SLA tidak terpenuhi",
            "Monitoring dan reporting",
            "Pengecualian SLA"
        ]
    },
    {
        "filename": "policy_privacy.md",
        "title": "Kebijakan Privasi dan Data",
        "type": "policy",
        "sections": [
            "Pengumpulan data",
            "Penggunaan data pelanggan",
            "Keamanan data",
            "Hak pelanggan",
            "Cookies dan tracking",
            "Third-party sharing"
        ]
    },
    {
        "filename": "guide_troubleshooting.md",
        "title": "Panduan Troubleshooting Koneksi",
        "type": "guide",
        "sections": [
            "Koneksi lambat",
            "Internet tidak bisa akses",
            "WiFi sering disconnect",
            "Modem tidak bisa menyala",
            "IP conflict",
            "DNS issues"
        ]
    },
    {
        "filename": "guide_router_setup.md",
        "title": "Panduan Setup Router",
        "type": "guide",
        "sections": [
            "Login ke router",
            "Mengubah password WiFi",
            "Mengubah nama SSID",
            "Setting channel WiFi",
            "Port forwarding",
            "Parental control"
        ]
    },
    {
        "filename": "guide_speed_test.md",
        "title": "Panduan Speed Test Internet",
        "type": "guide",
        "sections": [
            "Cara melakukan speed test",
            "Tools speed test yang direkomendasikan",
            "Interpretasi hasil speed test",
            "Faktor yang mempengaruhi kecepatan",
            "Kapan waktu terbaik untuk speed test",
            "Melaporkan hasil speed test"
        ]
    },
    {
        "filename": "terms_service.md",
        "title": "Syarat dan Ketentuan Layanan",
        "type": "terms",
        "sections": [
            "Definisi istilah",
            "Hak dan kewajiban pelanggan",
            "Hak dan kewajiban provider",
            "Batasan penggunaan",
            "Konten terlarang",
            "Pengakhiran layanan"
        ]
    },
    {
        "filename": "warranty_equipment.md",
        "title": "Garansi Perangkat",
        "type": "terms",
        "sections": [
            "Cakupan garansi",
            "Masa garansi",
            "Proses klaim garansi",
            "Pengecualian garansi",
            "Perpanjangan garansi",
            "Penggantian perangkat"
        ]
    },
    {
        "filename": "upgrade_procedure.md",
        "title": "Prosedur Upgrade Layanan",
        "type": "sop",
        "sections": [
            "Syarat upgrade",
            "Paket yang tersedia untuk upgrade",
            "Proses pengajuan",
            "Waktu aktivasi",
            "Perubahan billing",
            "Rollback prosedur"
        ]
    },
    {
        "filename": "network_coverage.md",
        "title": "Cakupan Jaringan",
        "type": "info",
        "sections": [
            "Area coverage",
            "Cek ketersediaan layanan",
            "Ekspansi jaringan",
            "Jaringan fiber optik",
            "Coverage map",
            "Area yang belum terjangkau"
        ]
    }
]