def test_import_trax_api_smoke():
    import trax

    expected = [
        "generate_keypair",
        "generate_nonce",
        "hash32",
        "derive_session_id",
        "create_admission_envelope_v1",
        "verify_admission_envelope_v1_for_receiver",
        "decode_admission_envelope_v1",
        "LocalDag",
    ]
    for name in expected:
        assert hasattr(trax, name)

    keypair = trax.generate_keypair()
    private_key = keypair["private_key"]
    public_key = bytes(keypair["public_key"])
    assert isinstance(public_key, bytes)
    assert len(public_key) == 32
    assert "PrivateKey" in repr(private_key)
    assert public_key.hex() not in repr(private_key)

    digest = bytes(trax.hash32(b"hello"))
    assert len(digest) == 32

    nonce = bytes(trax.generate_nonce())
    assert len(nonce) == 16
