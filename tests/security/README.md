# Security regression tests

Each `test_<id>_<what>.py` here is the regression test for one security finding, named by the finding's audit ID.
Its module docstring says what the attack was, how the test rebuilds it, and what the fix does. This file maps each
ID to its test modules, the code the test guards and the commits that fixed it, grouped by subsystem. To find every
test for a module, search this file for its name (for example `keepers.py` or `server.py`).

Where the IDs come from:
- **Earlier audits** (H, M, L, N, NEW, I, R and T): the October 2026 audits, fixed before 0.1.4. A suffix `-R`,
  `-R1` or `-R2` marks a residual found when the fix was re-tested, and "follow-up" a later fix to the same
  finding. Their write-ups weren't kept; the test docstrings are the record. The fixes were made on `fix/*` and
  `sec/*` branches, merged through `security/audit-2026-10` and released in 0.1.4. The commits named are those
  branches' commits, all in the history of the `v0.1.4` tag.
- **0.1.5 audit** (CONT, DHT, API, FEAT, NET and SUP): `0.1.5-audit.md`, fixed in 0.1.6 (`0.1.6-plan.md`).
- **0.2.0 review** (REV): a read-only review of 0.2.0, fixed before 0.2.0 was released; the findings and how each
  was built are the roadmap's "Security fixes from the 0.2.0 review".
- **0.2.0 release audit** (REL): reviews of every subsystem against remote, content and local attackers, fuzzing,
  and a soak of the cloud nodes, after the hardware tests of October 2026, fixed before 0.2.0 was released. The
  findings that aren't attacks (REL-5, 6, 7, 10, 11, 12 and 16) are in CHANGELOG.md, with their tests outside this
  directory; the rest are below.
- **0.2.1 audit** (AUD): reviews of every subsystem against remote, content and local attackers and of the
  packaging, before 0.2.1 was released, each finding rebuilt offline; IDs carry the finding's severity (`AUD-H1`
  high, `AUD-M4` medium, `AUD-L6` low). The findings that aren't attacks (AUD-M7, M8, M9 and L11) are in
  CHANGELOG.md, with their tests outside this directory; the rest are below.
- **No ID:** tests from an earlier review or from the security tooling, listed under the subsystem they guard.

Test modules are in this directory unless a path says otherwise. A commit marked "(test)" only added or updated
the test; the others changed the code. "Pages" are the NomadNet companion pages in `companion-nomadnet-sites/`.

Run them with `python3 -m unittest discover -s tests/security -p "test_*.py"`. Those needing real RNS
(`test_h1_real_rns.py`, `test_m1_real_rns.py` and the real-daemon part of `test_h2_display_page_pins.py`) run only
with `RNFS_E2E=1`, and `test_fuzz_parsers.py` only where hypothesis is installed; see CONTRIBUTING.md.

Shared fakes and helpers (not test modules): `_content_helpers.py` (a DAG declaring 1 byte but holding megabytes,
and two offline nodes counting what B downloads), `_server_helpers.py` (fake links and a `RequestServer` on a
FakeClock), `_fetchr2_helpers.py` (padded and chained DAGs, and a byte-range read's allowance),
`_dhtr2b_helpers.py` (SybilNet and the keeper scenario), `_pages_helpers.py` (the pages' own code against an offline node
and a fake CLI), and `_h1_real_helpers.py` and `_m1_real_helpers.py` (the real-RNS hub and attacker processes).

## Content, storage and fetching

| ID | Test modules | Code guarded | Fix commits |
|---|---|---|---|
| L3 | `test_l3_zero_size_children.py`, `test_dag_caps.py` | `rnfs/fetcher.py` (`dag_bound`, `node_credit`, `fetch_dag`) | `211c470`, `4bdd14a` (test) |
| N1 | `test_n1_user_pin_unbounded.py`, `test_n1_display_page_unbounded.py`, `test_dag_caps.py` | `rnfs/fetcher.py` (`fetch_dag`), `rnfs/node.py` (`pin_add`), display-mu-file page | `211c470`, `6b23f35` (page) |
| N1-R1 | `test_n1_read_range_cap.py` | `rnfs/fetcher.py` (`Fetch.read_range`) | `2290b66` |
| N1-R2 | `test_n1_listing_chains.py` | `rnfs/fetcher.py` (`fetch_listing`) | `2290b66` |
| N1-R3 | `test_n1_entry_credit.py` | `rnfs/fetcher.py` (`node_credit`) | `2290b66` |
| L8 | `test_l8_folder_swap.py`, `test_l8_path_walk_checks.py` | `rnfs/content/builder.py`, `rnfs/content/shared_folder.py`, `rnfs/fsutil.py` | `b77bf1a`, `b18fb02` (test) |
| L8-R2 | `test_l8r2_hard_links.py` | `rnfs/content/shared_folder.py`, `rnfs/content/builder.py` | `1d8e099` |
| L9 | `test_l9_get_free_space.py`, `test_l9_free_space_unknown.py` | `rnfs/node.py` (`_check_free_space`), `rnfs/node_content.py`, `rnfs/content/storage.py` | `7640d0b`; failing closed: `383f770` |
| NEW-5 | `test_new5_storage_lock_walks.py` | `rnfs/content/storage.py` (`evict`, `gc`) | `6f6b36e` |
| CONT-1 | `test_cont1_deny_hash_amplification.py` | `rnfs/denylist.py`, `rnfs/content/walker.py`, `rnfs/node_content.py`, `rnfs/node.py` (`_check_file`) | `5454d5d` |
| CONT-2 | `test_cont2_write_out_swap.py` | `rnfs/content/walker.py` (`write_out`), `rnfs/node.py` (`get`), `rnfs/winacl.py` | `04b3e2e` |
| CONT-3 | `test_cont3_shared_folder_target.py` | `rnfs/content/shared_folder.py` (`SharedFolder.scan`), `rnfs/node.py` | `3fb49ae`, `8d30c61` |
| CONT-4 / API-6 | `test_cont4_denied_hash_notice.py` | `rnfs/node.py` (`_check_file`); documented in specification section 8 | `c61a324` |

## DHT, providers and names

| ID | Test modules | Code guarded | Fix commits |
|---|---|---|---|
| M3 | `test_m3_keeper_discovery.py` | `rnfs/features/keepers.py`, `rnfs/dht/lookup.py`, `rnfs/dht/routing.py`, `rnfs/fetcher.py` | `424b20b` |
| M3-R1 | `test_m3r1_keeper_standing.py` | `rnfs/features/keepers.py` (`KeepersFeature.ask_keepers`), `rnfs/dht/lookup.py`, `rnfs/dht/routing.py` | `c83ceab`, `e2e78c2` (test) |
| M3-R2 | `test_m3r2_keeper_unknown_budget.py` | `rnfs/features/keepers.py`, `rnfs/dht/lookup.py` | `c83ceab` |
| M4 | `test_m4_provider_admission.py` (rebuilt for 0.2.0's record budget and tiers) | `rnfs/dht/providers.py` (`ProviderStore`: `_reclaim`), `rnfs/network/protocol.py`, `rnfs/node.py` | `641b9b2` |
| M4-R | `test_m4r_newcomer_pool.py` (rebuilt for 0.2.0: the Newcomer tier) | `rnfs/dht/providers.py` (`_reclaim`, `_largest`, the standing kept by `_remove`) | `2f72d80` |
| NEW-1 | `test_new1_publisher_withdraw_race.py` | `rnfs/publisher.py` (`publish_due`, `refresh`) | `c3b4d45` |
| NEW-4 | `test_new4_private_mode_records.py` | `rnfs/dht/providers.py`, `rnfs/node.py` (`NodeBackend.find`), `rnfs/network/links.py` (`LinkPool`) | `1ffc8eb` |
| NEW-6 | `test_new6_publisher_crash_cut_round.py`; remote-pin half: `test_new6_remote_pins_restart.py`; follow-up: `test_new6_remote_pins_bad_entry.py` | `rnfs/publisher.py`; `rnfs/node.py` (`_load_remote_pins`, `_resume_remote_pins`), `rnfs/statedb.py` | `c3b4d45`; `719c1bc`; `49df3c9` |
| NEW-8 | `test_new8_provide_at_limit.py` (rebuilt for 0.2.0: a full budget) | `rnfs/dht/providers.py` (`ProviderStore.provide`, `_store`) | `d74dcc5` |
| L4 | `test_l4_name_rollback.py` | `rnfs/names.py` (`NameManager`, the name table) | `97b2537` |
| L4-R | `test_l4r_name_floor.py` | `rnfs/names.py` (the gateway's name floor) | `a9c5640` |
| NEW-2 | `test_new2_name_publish_crash.py`, `test_new2_follow_lxmf_pins.py` | `rnfs/names.py` (`NameManager.publish`), `rnfs/node.py`, `rnfs/features/follow.py`, `rnfs/content/storage.py` | `8ecad5b`, `ef3cc9d` |
| R-2 | `test_r2_name_key_unreadable.py` | `rnfs/names.py` (`release_gone`, `reconcile_pins`) | `56ed5ac` |
| T1 | `test_t1_parse_name_whitespace.py`; residual: `test_t1_parse_hex_id_whitespace.py` | `rnfs/names.py` (`parse_name`); `rnfs/api.py` (`parse_hex_id`) | `63ebae9`; `bb23b64` |
| DHT-1 | `test_dht1_keeper_query_budget.py` | `rnfs/features/keepers.py`, `rnfs/dht/lookup.py` | `73c8295` |
| DHT-2 | `test_dht2_replayed_announce.py`; also `tests/test_names.py` | `rnfs/names.py` (`RnsNetwork.recalled_emission`) | `c31372b` |
| DHT-4 | `test_dht4_private_name_announces.py`; also `tests/test_public.py` | `rnfs/names.py` (`NameManager.publish`), `rnfs/cli.py` | `509c056` |
| DHT-5 | `test_dht5_own_provider_record.py`; also `tests/test_providers.py` | `rnfs/dht/providers.py` (`_key_room`, `_reclaim`, the own tier) | `4b72ea7` |
| DHT-6 | none: documented only (the cost of grinding node IDs, specification section 10) | none | `4441878` (docs) |
| none (RNS blackhole) | `test_rns_blackhole.py`; also `tests/test_blackhole.py`, `tests/test_rns_compat.py` | `rnfs/network/blackhole.py` (`Blackhole`, `listed`), `rnfs/dht/routing.py` (`RoutingTable.blackholed`), `rnfs/node.py` (`maintain`), `rnfs/network/server.py` (`RequestServer.blackholed`) | `963205e` |

## Network and the `/blocks` server

| ID | Test modules | Code guarded | Fix commits |
|---|---|---|---|
| H1 | `test_h1_response_slots.py`, `test_h1_real_rns.py` (`RNFS_E2E`), `test_rns_compat_security.py` | `rnfs/network/server.py` (`ResponsePools`, `RequestServer`), `rnfs/config.py` | `dbd1144`, `7b9f6b9` and `c8d09ad` (test) |
| H1 (follow-up) | `test_h1_regressions.py` | `rnfs/network/server.py` (`link_path`, the grace and fairness rules), `rnfs/config.py` | `126063d`, `c6f4a74`, `86c4475`, `90a2952` (test) |
| M1 | `test_m1_half_open_links.py`, `test_m1_real_rns.py` (`RNFS_E2E`), `test_rns_compat_security.py` | `rnfs/network/guards.py` (`limit_half_open`) | `09c6324` |
| M2 | `test_m2_upload_budget.py` | `rnfs/network/server.py` (upload_budget), `rnfs/mesh.py` | `0726417` |
| M2 (follow-up) | `test_m2_commitment_bound.py` | `rnfs/network/server.py` | `1af981f` |
| L1 | `test_l1_response_precheck.py`, `test_rns_compat_security.py` | `rnfs/network/guards.py`, `rnfs/network/protocol.py` (`response_refusal`) | `73041a7` |
| L2 | `test_l2_blocks_request_limits.py` | `rnfs/network/protocol.py` (a repeated CID), `rnfs/network/server.py` (the budget with no rate estimate) | `80b950d`, `a98b985` |
| L17 | `test_l17_rejected_responses.py` (L17a), `test_l17_request_rate.py` (L17b), `test_rns_compat_security.py` | `rnfs/network/server.py` (the cooldown; `RequestServer`'s per-link request rate) | `42aadca` (L17a), `6c79d4c` (L17b) |
| NEW-3 | `test_new3_remote_pin_wakeup.py` | `rnfs/node.py` (`_remote_pin_worker`) | `719c1bc` |
| NET-1 | `test_net1_cancelled_responses.py`, `test_l17_rejected_responses.py` | `rnfs/network/server.py` (`NO_PROGRESS_FRACTION`) | `0ec517b` |
| NET-2 | `test_net2_single_packet_pacing.py`; also `tests/test_pacing.py`, `tests/test_server.py` | `rnfs/network/server.py` (pacing under `max_upload_rate`) | `c0ec333` |
| NET-3 | `test_net3_outgoing_requests.py`, `test_l1_response_precheck.py` | `rnfs/network/guards.py` (`guarded_receive`) | `81afb98` |
| NET-4 | `test_net4_dripping_provider.py`; also `tests/test_fetcher.py` (`ReadAheadLinkTest`) | `rnfs/fetcher.py` (end-game duplication of a batch in flight past `deadline_slack`), `rnfs/network/client.py` and `rnfs/network/links.py` (`close_unless_used`) | `afa3820`, `ac9ed4f` |
| NET-5 | `test_net5_proof_wait.py` | `rnfs/network/server.py` (`RequestServer._proof_wait`) | `db27b8b` |
| none (NomadNet links) | `test_nomadnet_link_bounds.py`; also `tests/test_nomadnet.py`, `tests/test_nomadnet_e2e.py` (`RNFS_E2E`) | `rnfs/network/nomadnet.py` (`Transfer`, `segment_refusal`), `rnfs/network/guards.py` (`mark_nomadnet`, the page check), `rnfs/network/protocol.py` (`page_response_refusal`, `metadata_refusal`) | `4e9c97b`, `2b2beb4` (built with NomadNet add, 0.2.0) |
| none (`/range`) | `test_range_decompression.py`, `test_range_limits.py`; also `tests/test_ranges.py`, `tests/test_client.py` (`test_range_*`), `tests/test_wire.py` (`RangeVectorTest`) | `rnfs/network/protocol.py` (`decompress_range`, `parse_range_request`, `parse_range_response`, `_range_answer_end`), `rnfs/network/server.py` (`_handle_range`, `_serve`, `CompressionBudget`), `rnfs/network/client.py` (`block_range`) | `02724ae`, `0c56e10` |
| none (RNS internals) | `test_rns_compat_security.py` | the RNS attributes `rnfs/network/server.py` and `rnfs/network/guards.py` rely on (H1, M1, L1, L17) | `dbd1144`, `09c6324`, `73041a7`, `42aadca`, `90a2952` (test) |

## Local API, client and gateway

| ID | Test modules | Code guarded | Fix commits |
|---|---|---|---|
| I1 | `test_i1_507_message.py` | `rnfs/gateway.py` (the 507 message), `rnfs/content/storage.py` (`StorageFull`) | `68dcb8f` |
| L6 | `test_l6_gateway_storage_oracle.py` | `rnfs/gateway.py`, `rnfs/gateway_formats.py` (`?format=`) | `a678443`, `3265621`, `4a765d4` (documented limit) |
| NEW-7 | `test_new7_api_status_open.py` | `rnfs/api.py` (`GET /api/status`), `rnfs/cli.py` | `863f0b7` |
| none (CORS) | `test_gateway_cors.py` | `rnfs/gateway.py`, `rnfs/gateway_formats.py` | `d77be66` |
| none (site hosts) | `test_gateway_site_hosts.py`; also `tests/test_gateway_sites.py` | `rnfs/gateway.py` (`parse_host`, `serve_site`, the site CSP), `rnfs/content/cid.py` (`from_base36`) | `1883af2` (built with the subdomain gateway, 0.2.0) |
| none (headers) | `test_response_headers.py` | `rnfs/gateway.py` (`?filename=`), `rnfs/api.py` (`no-store`, `nosniff`) | `bfe9bc7` (test; pins existing behaviour) |
| API-2 | `test_api2_proof_first.py` | `rnfs/apiclient.py` (`verify_daemon`, `PROOF_TIMEOUT`) | `dda4603` |
| API-3 | none: documented only (the proof keep-open, specification section 11) | none | `feba0f0` (docs) |
| API-4 | `test_api4_old_daemon_fallback.py`; also `tests/test_apiclient.py` | `rnfs/apiclient.py` (the pre-0.1.5 reconnect, dropped in 0.2.0: such daemons are refused in `check_daemon`) | `b565ee8` |
| API-5 | `test_api5_head_deny.py` | `rnfs/node_content.py` (`stored_head`), `rnfs/gateway.py`, `rnfs/node.py` | `cb5c93f` |
| API-7 | `test_api7_missing_fetch_metadata.py` | `rnfs/gateway.py` (`limited`, the User-Agent rule) | `0d010b1` |

## Features: follow

The LXMF feature's findings (L12, L12-R, L13, L13-R, FEAT-1 to FEAT-5 and the auto-fetch fairness fix) and their
regression tests left rnfs with the feature in 0.2.0, when LXMF messaging became an app of its own; R-1 and NEW-2
keep their follow tests.

| ID | Test modules | Code guarded | Fix commits |
|---|---|---|---|
| L5 | `test_l5_follow_cap.py` | `rnfs/features/follow.py`, `rnfs/fetcher.py` (`dag_bound`) | `211c470` |
| R-1 | `test_r1_reconcile_damaged_state.py` | `rnfs/features/follow.py` (the start-up reconcile) | `0e350ae` |
| DHT-3 | `test_dht3_follow_quota.py`, `test_l5_follow_cap.py` | `rnfs/features/follow.py` (`follow_quota`), `rnfs/config.py` | `63a0157` |
| FEAT-6 | `test_feat6_printable_label.py` | `rnfs/fsutil.py` (`printable_label`) | `0c97eb7` |
| FEAT-7 | `test_feat7_example_and_client_escaping.py` | `examples/rnfs_share.py`, `rnfs/remote.py` (`Client._report`) | `71d7cb2` |

## Companion pages

| ID | Test modules | Code guarded | Fix commits |
|---|---|---|---|
| H2 | `test_h2_display_page_pins.py` (part `RNFS_E2E`) | display-mu-file page | `6b23f35` |
| H2-R | `test_h2r_display_page_private_and_rate.py` | display-mu-file page (private mode, `VISITOR_LIMIT`) | `3707b53` |
| L10 | `test_l10_index_page_injection.py` | both pages' `escape()` | `9098381`, `6b23f35` |
| L11 | `test_l11_display_page_micron.py` | display-mu-file page (`plain_text`, the allowlist) | `6b23f35` |
| L14 | `test_l14_page_local_paths.py` | both pages (`log`, which writes page.log; display-mu-file's `fail`) | `6b23f35`, `9098381` |
| M6 | `test_m6_index_page_names.py`, `test_m6_pin_ls_sealed.py`; follow-up: `test_m6_index_page_full_run.py` | file-index-node page (`entry`, `shown`, `is_sealed`); `rnfs/node.py` (`pin_ls`), `rnfs/cli.py` | `9098381`, `9544fd6`; `5092f80` |
| FEAT-8 | `test_feat8_index_page_cache.py` | file-index-node page (`take_lock`, `cached`) | `be571fc` |

N1's page half is in the content table.

## Platform: files, permissions and Windows

| ID | Test modules | Code guarded | Fix commits |
|---|---|---|---|
| L7 | `test_l7_windows_acl.py` | `rnfs/winacl.py`, `rnfs/fsutil.py`, `rnfs/network/identity.py`, `rnfs/names.py`, `rnfs/node.py`, `rnfs/cli.py` | `1d0b0e6`, `2be6acf` |
| L7-R1 | `test_l7r1_windows_dir_creation.py` | `rnfs/winacl.py` (`create_private_dir`), `rnfs/config.py`, `rnfs/fsutil.py`, `rnfs/node.py` | `4383bfa` |
| L7-R2 | `test_l7r2_junction_root.py` | `rnfs/fsutil.py`, `rnfs/node.py` (the root check) | `4383bfa` |
| I2 | `test_i2_identity_symlink.py`; packaging half: `test_i2_packaging_hardening.py` | `rnfs/network/identity.py` (`load_or_create_identity`); `.github/workflows/release.yml`, `contrib/rnfs.service`, `install.sh` | `ea88d13`; `cbf257e` |
| I2-R1 | `test_i2r1_identity_file.py` | `rnfs/network/identity.py` (`IdentityFileError`) | `2db7830` |
| API-1 | `test_api1_settings_file.py`, `test_api1_subdirs.py` | `rnfs/config.py`, `rnfs/fsutil.py`, `rnfs/node.py`, `rnfs/embedded.py`, `rnfs/cli.py`, `rnfs/apiclient.py` | `939ecae` |

## Packaging and release

| ID | Test modules | Code guarded | Fix commits |
|---|---|---|---|
| L15 | `test_l15_install_release_check.py` | `install.sh` (`download`, `verify_release`, `fetch_release`) | `e2b5686` |
| L16 | `test_l16_release_container.py` | `.github/workflows/release.yml` (the Debian 12 smoke test) | `1f3cb70` |
| SUP-1 | `tests/test_packaging.py` (`test_flatpak_builds_skip_the_actions_cache`) | `.github/workflows/release.yml` | `6fd0cc7` |
| SUP-2 | `test_l15_install_release_check.py`; also `tests/test_packaging.py` (`test_release_wheel_is_of_the_tag`, `test_provenance_names_the_tag`) | `install.sh` | `67bfafe` |
| SUP-3 | `test_l15_install_release_check.py` | `install.sh` | `c64ebf5` |
| SUP-4 | none: the PyPI name is the maintainer's to claim; the code side is a README note | `README.md` | `4ac4b89` (docs) |
| SUP-5 | `tests/test_packaging.py` (`test_flatpak_sandbox_is_narrow`) | `packaging/flatpak/io.github.afit21.rnfs.yml` | `021c7c2` |
| SUP-6 | `tests/test_packaging.py` (`test_unit_hardening_is_an_opt_in_drop_in`, `test_deb_postinst_python_is_isolated`, `test_plain_http_source_warns`) | `contrib/rnfs.service`, `contrib/rnfs.service.d/hardening.conf`, `packaging/build-deb.sh`, `install.sh` | `c00f8d7` |

## Across subsystems

| ID | Test modules | Code guarded | Fix commits |
|---|---|---|---|
| none (fuzzing) | `test_fuzz_parsers.py` (needs hypothesis) | the strict parsers of untrusted bytes: `rnfs/content/cid.py`, `rnfs/content/dag.py`, `rnfs/content/storage.py`, `rnfs/names.py`, `rnfs/network/`, `rnfs/uris.py`, `rnfs/gateway.py`; 0.2.0's: `rnfs/network/protocol.py` (`/range`: `parse_range_request`, `parse_range_response`, `decompress_range`, `is_range_unserved`; the prechecks `body_refusal`, `response_refusal`, `page_response_refusal`, `metadata_refusal`), `rnfs/network/server.py` (the `/range` handler), `rnfs/network/client.py` (`block_range`), `rnfs/ranges.py` (partial blocks, `RangeSupport`'s saved rows), `rnfs/network/guards.py` (`_page_refusal`), `rnfs/network/nomadnet.py` (`segment_refusal`, `_sent_name`), `rnfs/nomadnet.py` (`parse`, `snapshot_name`, `is_not_allowed`), and the saved rows of `rnfs/dht/routing.py` (`restore`), `rnfs/dht/firstseen.py` and `rnfs/dht/scores.py` | `077f33a`, `6ed1751` (tests); found T1, and in 0.2.0 a score past its limit (`41c60df`) |

## 0.2.0 review (REV)

A read-only review of 0.2.0 against remote and local attackers, fixed before 0.2.0 was released; the findings are the roadmap's "Security fixes from the 0.2.0 review".

| ID | Test modules | Code guarded | Fix commits |
|---|---|---|---|
| REV1 | `test_rev1_add_rnfs_dir.py` | `rnfs/node.py` (`add_path`), `rnfs/content/shared_folder.py` (`add_refused_reason`, `private_dirs`, the scan), `rnfs/content/builder.py` (`BuildOptions.skip_dirs`, `_build_tree`, `build_path`), `rnfs/content/tarstream.py` (`folder_chunks`), `rnfs/node_content.py` (`add_sealed_path`), `rnfs/name_sync.py`, `rnfs/features/jobs.py` (`submit`) | `5195153` |
| REV2 | `test_rev2_provider_write_budget.py`; also `tests/test_providers.py` (`WriteBudgetTest`) | `rnfs/dht/providers.py` (`_WriteBudget`, `ProviderStore._store`, `_withdraw`, `REFRESH_WRITE`) | `63201f5` |
| REV3 | `test_rev3_full_key_takeover.py`; also `tests/test_providers.py` (`KeyCapTest`) | `rnfs/dht/providers.py` (`ProviderStore._key_room`, `KEY_NEWCOMER_SHARE`) | `18897a5` |
| REV4 | `test_rev4_forced_partition.py` | `rnfs/dht/local.py` (`PartitionMode.failed`, `probe_target`), `rnfs/node.py` (`_knows_server`, `_run_local`) | `53ed603` |
| REV5 | `test_rev5_nomadnet_page_memory.py` | `rnfs/network/nomadnet.py` (`MAX_PAGE_SIZE`, `Transfer.resource_started`, `_outcome`) | `ac38138` |
| REV6 | `test_rev6_site_upload_render.py` | `rnfs/gateway.py` (`serve_target`, `serve_file`, `file_type`, `SITE_SNIFFED_TYPES`) | `abf8ab0` |
| REV7 | `test_rev7_firstseen_seniors.py` | `rnfs/dht/firstseen.py` (`FirstSeenLog._evict`, `_victim`), `rnfs/node.py` (the log's settings) | `1109f77` |
| REV8 | `test_rev8_size_estimate_inflation.py` | `rnfs/dht/density.py` (`verified_sample`), `rnfs/dht/lookup.py` (`Lookup._sample`, `verified`), `rnfs/dht/disjoint.py` (`DisjointLookup._sample`) | `c6746e2` |
A read-only review of 0.2.0 against remote and local attackers, fixed before 0.2.0 was released; the findings are
the roadmap's "Security fixes from the 0.2.0 review".

| ID | Test modules | Code guarded | Fix commits |
|---|---|---|---|
| REV9 | `test_rev9_app_name_oracle.py`; also `tests/test_api.py` (`test_proof_for_an_app`) | `rnfs/apps.py` (`AppRegistry.proof_key`), `rnfs/api.py` (`ApiHandler.proof_key`, `send_common_headers`) | `284268e` |
| REV10 | `test_rev10_sealed_unpack.py`; also `tests/test_tar_stream.py` (`test_scan_counts_allocation_units`, scan_tar agreeing with build_tar), `tests/test_fsutil.py` (`AllocationUnitTest`) | `rnfs/node_content.py` (`unpack_sealed`, `_check_unpack_space`, `_unpacking`, `collect_unpack_leftovers`), `rnfs/content/tarstream.py` (`scan_tar`, `Extent`), `rnfs/fsutil.py` (`allocation_unit`), `rnfs/node.py` (`_setup`) | `76b5af4` |
| REV11 | `test_rev11_standings_cap.py`; also `tests/test_providers.py` (`test_departed_standings_are_capped_on_disk`) | `rnfs/dht/providers.py` (`ProviderStore._cap_departed`, `maintain`, `MAX_DEPARTED`) | `f5dd6e8` |

## 0.2.0 release audit (REL)

| ID | Test modules | Code guarded | Fix commits |
|---|---|---|---|
| REL-1 | `test_rel1_alternating_unreachable.py` | `rnfs/fetcher.py` (`Fetch._handle`: the lost count and the unreachable retry) | `1a34a69` |
| REL-2 | `test_rel2_unserved_displacement.py` | `rnfs/network/server.py` (`RequestServer._serve`, `Backend.serves`), `rnfs/node.py` (`NodeBackend.serves`) | `4288e10` |
| REL-3 | `test_rel3_request_shape.py` | `rnfs/network/guards.py` (`guarded_handle_request`, `install`) | `744e8cb` |
| REL-4 | `test_rel4_negative_tar_size.py` | `rnfs/content/tarstream.py` (the `Header` class: `_proc_member`, `fromtarfile`) | `3849a43` |
| REL-8 | `test_rev1_add_rnfs_dir.py` (`test_a_file_in_another_name_for_the_rnfs_dir_is_refused`) | `rnfs/content/shared_folder.py` (`add_refused_reason`, `_reached_otherwise`) | `61b67c3` |
| REL-9 | `test_rel9_gateway_document_cache.py` | `rnfs/gateway.py` (`for_type`, `DOCUMENT_TYPES`, `send_not_modified`) | `3c84f4a` |
| REL-17 | `test_rel17_fuzz_findings.py` | `rnfs/gateway.py` (`type_from_name`, `respond_error`, `redirect_to_slash`), `rnfs/api.py` (`parse_request`, the JSON body), `rnfs/config.py` (`_is_number`, the whole-number checks), `rnfs/mesh.py` (`validate`), `rnfs/content/tarstream.py` (`_Ending`, `build_tar`, `scan_tar`) | `5026998` |
| REL-13 | `test_rel13_withdraw_restore_budget.py` | `rnfs/dht/providers.py` (`ProviderStore._withdraw`, `_store`, `_WriteBudget.refund`) | `e216954` |
| REL-14 | `test_rel14_keeper_below_floor.py` | `rnfs/names.py` (`NameManager.known_sequence`), `rnfs/features/keepers.py` (`_store`, `_known_sequence`) | `29b95b4` |
| REL-15 | `test_rel15_clockskew_flood.py` | `rnfs/clockskew.py` (`ClockSkew._sample`, `_estimate`), `rnfs/node.py` (`_announced`) | `6d01750` |
| REL-20 | `test_rel20_range_renege.py` | `rnfs/ranges.py` (`Ranges._fetch`: `BlocksResult.reneged`), `rnfs/fetcher.py` (`Fetch._handle`) | `0c5fa0d` |

## 0.2.1 audit (AUD)

| ID | Test modules | Code guarded | Fix commits |
|---|---|---|---|
| AUD-H1 | `test_audh1_paced_answer_slot.py` | `rnfs/network/server.py` (`RequestServer._serve`: `slotted`, `unslotted`, `_untrack`, `_link_closed`) | (uncommitted) |
| AUD-H2 | `test_audh2_ipv6_loopback_squat.py` | `rnfs/api.py` (`LocalServer._bind`, `BoundedHTTPServer` sibling slots, `server_bind`, `NO_IPV6`, `PORT_TAKEN`), `rnfs/gateway.py` (`GatewayServer`) | (uncommitted) |
| AUD-H3 | `test_audh3_add_swapped_leaf.py` | `rnfs/node.py` (`RnfsNode.add_path`), `rnfs/content/builder.py` (`check_root`, `BuildOptions.root_check`), `rnfs/content/shared_folder.py` (`opened_refused_reason`, `opened_path`), `rnfs/node_content.py` (`add_sealed_path`) | (uncommitted) |
| AUD-H4 | `test_audh4_dripping_probe.py` | `rnfs/fetcher.py` (`Fetch._assign`, `_probes_overdue`, `_discover`, `_duplicate`, `_settle_duplicate`, `_handle`, `_expected_time`), `rnfs/network/client.py` (`PeerClient.blocks`, `block_range`: `slot_wait`, `NO_SLOT`), `rnfs/network/links.py` (`LinkPool.request`, `Exchange.no_slot`), `rnfs/ranges.py` (`Ranges.fetch`) | (uncommitted) |
| AUD-M1 | `test_audm1_size_estimate_poisoning.py`; also `tests/test_search.py` (`test_size_samples_only_from_keys_the_node_chose`) | `rnfs/dht/lookups.py` (`Lookups.search`, `publish`, `flags`, `widen`, `sample_due`), `rnfs/dht/density.py` (`SizeEstimate.forget`), `rnfs/node.py` (`_sample_sizes`, `_join`) | (uncommitted) |
| AUD-M2 | `test_audm2_rerun_query_cap.py` | `rnfs/dht/lookup.py` (`Lookup._iterate`: runs share `max_queries`), `rnfs/fetcher.py` (`Fetch._lookup_found`, `_add_provider`, `_rerun_lookup`) | (uncommitted) |
| AUD-M3 | `test_audm3_probe_introducer.py` | `rnfs/dht/local.py` (`PartitionMode.failed`, `_targets`, `probe_target`, `probed`), `rnfs/dht/lookup.py` (`Lookup.introducers`), `rnfs/node.py` (`_new_lookup`, `_query_failed`, `_is_senior`) | (uncommitted) |
| AUD-M4 | `test_audm4_slow_pool_runs.py` | `rnfs/network/server.py` (`ResponsePools._displaceable`, `_run_start`, `_ended`, `runs`; `Response.run_start`, `fast_interface`; `RequestServer._serve`) | (uncommitted) |
| AUD-M5 | `test_audm5_pairing_preempt.py`; also `tests/test_apps.py` (`test_deny_expiry_and_refusals`) | `rnfs/apps.py` (`PairingRequests.request`, `approve`, `MAX_PENDING`), `rnfs/api.py` (`api_app_approve`), `rnfs/apiclient.py` (`app_approve`), `rnfs/cli.py` (`run_app_approve`) | (uncommitted) |
| AUD-M6 | `test_audm6_entry_cap.py`, `test_audm6_follow_deny_entry_cap.py`, `test_n1_entry_credit.py` | `rnfs/node_pins.py` (`pin_add`, `_remote_pin_fetch`), `rnfs/node.py` (`dir_set`, `dir_rm`), `rnfs/features/follow.py` (`FollowFeature.check`), `rnfs/features/deny.py` (`DenyFeature.check`), `rnfs/fetcher.py` (`_Walk.met`: `.entries`) | (uncommitted) |
| AUD-M10 | `test_audm10_install_venv.py` | `install.sh` (`venv_ok`, `do_install`, `install_cleanup`, `recover_venv`), the Flatpak manifest's comments | (uncommitted) |
| AUD-M11 | `test_audm11_get_allocation.py` | `rnfs/node.py` (`RnfsNode.get`, `_check_write_space`), `rnfs/content/tarstream.py` (`dag_extent`, `Extent.written`) | (uncommitted) |
| AUD-L1 | `test_audl1_pairing_name_oracle.py` | `rnfs/apps.py` (`PairingRequests.request`, `approve`) | (uncommitted) |
| AUD-L2 | `test_audl2_name_export_private_file.py` | `rnfs/cli.py` (`run_name_export`) | (uncommitted) |
| AUD-L4 | `test_audl4_range_compression_share.py` | `rnfs/network/server.py` (`CompressionBudget.take`, `forget`, `COMPRESS_LINK_SHARE`; `RequestServer._handle_range`, `_serve`) | (uncommitted) |
| AUD-L5 | `test_audl5_early_responses.py` | `rnfs/network/links.py` (`EarlyResponses.on_response`, `_one_packet`, `_prune`, `deliver`) | (uncommitted) |
| AUD-L6 | `test_audl6_path_turns.py` | `rnfs/network/paths.py` (`PathRequests._take_turn`, `_give_back`, `charge`; `_Turn`) | (uncommitted) |
| AUD-L7 | `test_audl7_link_identify_flood.py`; also `tests/test_rns_compat.py` (LINKIDENTIFY) | `rnfs/network/guards.py` (`guarded_receive`, `_identify_refused`, `MAX_IDENTIFY_PACKETS`) | (uncommitted) |
| AUD-L8 | `test_audl8_range_block_rereads.py` | `rnfs/content/storage.py` (`Storage.get(remember=)`, `_verified`, `_delete`, `VERIFIED_MEMORY`), `rnfs/node.py` (`NodeBackend.served_block`) | (uncommitted) |
| AUD-L9 | `test_audl9_client_announce_replay.py`; also `tests/test_node.py` (`test_announce_first_seen_log_is_kept_at_a_restart`) | `rnfs/node.py` (`_heard_client`, `_left_server_mode`), `rnfs/dht/firstseen.py` (`FirstSeenLog.last_heard`) | (uncommitted) |
| AUD-L10 | `test_audl10_malformed_renege.py` | `rnfs/ranges.py` (`Ranges._fetch`: `BlocksResult.reneged`), `rnfs/fetcher.py` (`Fetch._handle`) | (uncommitted) |
| AUD-L12 | `test_audl12_release_before_resume.py` | `rnfs/node.py` (`_start_network`, `_start_threads`), `rnfs/node_pins.py` (`_resume_remote_pins`, `remote_pin`) | (uncommitted) |
| AUD-L13 | `test_audl13_event_watch_backlog.py` | `rnfs/interface.py` (`_Events.post`, `_Backlog`, `Rnfs._deliver_event`) | (uncommitted) |
| AUD-L14 | `test_audl14_lock_audit.py` | `packaging/build-tools.lock`, `.github/workflows/audit.yml`, `.github/dependabot.yml` | (uncommitted) |
| AUD-L15 | `test_audl15_install_downgrade.py` | `install.sh` (`installed_version`, `check_downgrade`) | (uncommitted) |
| AUD-L16 | `test_audl16_windows_task.py` | `contrib/install-windows-task.ps1` | (uncommitted) |
| AUD-L17 | `test_audl17_strict_dropin.py` | `contrib/rnfs.service.d/strict.conf`, `hardening.conf`, the unit's comments | (uncommitted) |
| AUD-L18 | `test_audl18_reproducible_deb.py` | `packaging/build-deb.sh`, `.github/workflows/release.yml` (the python and deb jobs) | (uncommitted) |
