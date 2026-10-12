- **NFL:** a quarterback with no sack, rush or penalty play gets 0 for that `exp_qbr` input, as the QBR trainer
  did, instead of 0/0 = NaN, which the booster read as missing. Spearman of `exp_qbr` vs ESPN Total QBR over
  the 573 rated 2023 quarterback-games rises from 0.807 to 0.834. (#802)
